use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rand::{seq::IteratorRandom, RngCore};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::{mpsc, Mutex};
use tower_http::{services::ServeDir, trace::TraceLayer};
use uuid::Uuid;

const MAX_ROOM_SIZE: usize = 8;

type SharedState = Arc<Mutex<AppState>>;

#[derive(Clone)]
struct App {
    state: SharedState,
    auth_store: AuthStore,
}

#[derive(Clone)]
struct AuthStore {
    path: Option<PathBuf>,
}

#[derive(Default)]
struct AppState {
    users: HashMap<String, UserAccount>,
    sessions: HashMap<Uuid, Session>,
    rooms: HashMap<Uuid, Room>,
    directory: HashMap<Uuid, DirectoryEntry>,
    join_requests: HashMap<Uuid, JoinRequest>,
}

#[derive(Clone, Serialize, Deserialize)]
struct UserAccount {
    display_name: String,
    password_hash: String,
    created_at: DateTime<Utc>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Session {
    id: Uuid,
    display_name: String,
    account_email: Option<String>,
    created_at: DateTime<Utc>,
}

#[derive(Default, Serialize, Deserialize)]
struct AuthData {
    users: HashMap<String, UserAccount>,
    sessions: HashMap<Uuid, Session>,
}

#[derive(Clone, Serialize)]
struct DirectoryEntry {
    session_id: Uuid,
    display_name: String,
    room_id: Option<Uuid>,
    available: bool,
    updated_at: DateTime<Utc>,
}

#[derive(Clone)]
struct Room {
    id: Uuid,
    host_session: Uuid,
    host_controls_joiners: bool,
    share_link_enabled: bool,
    participants: HashSet<Uuid>,
    approved_joiners: HashSet<Uuid>,
    senders: HashMap<Uuid, mpsc::UnboundedSender<ServerWsEvent>>,
    waiting_for_random: bool,
    created_at: DateTime<Utc>,
}

#[derive(Clone, Serialize)]
struct JoinRequest {
    id: Uuid,
    room_id: Uuid,
    requester_session: Uuid,
    requester_display_name: String,
    status: JoinRequestStatus,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    accepted_by: Option<Uuid>,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
enum JoinRequestStatus {
    Pending,
    Accepted,
}

#[derive(Deserialize)]
struct SignupRequest {
    email: String,
    password: String,
    display_name: String,
}

#[derive(Deserialize)]
struct SigninRequest {
    email: String,
    password: String,
}

#[derive(Deserialize)]
struct SignoutRequest {
    session_id: Uuid,
}

#[derive(Deserialize)]
struct GuestRequest {
    display_name: Option<String>,
}

#[derive(Deserialize)]
struct StartRoomRequest {
    session_id: Uuid,
    size: Option<usize>,
    host_controls_joiners: Option<bool>,
    share_link_enabled: Option<bool>,
}

#[derive(Deserialize)]
struct DirectoryRequest {
    session_id: Uuid,
    display_name: String,
    room_id: Option<Uuid>,
    available: bool,
}

#[derive(Deserialize)]
struct AddRandomRequest {
    session_id: Uuid,
    room_id: Uuid,
}

#[derive(Deserialize)]
struct JoinRoomRequest {
    session_id: Uuid,
}

#[derive(Deserialize)]
struct JoinRequestsQuery {
    session_id: Uuid,
}

#[derive(Deserialize)]
struct WsParams {
    session_id: Uuid,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum ClientWsEvent {
    Signal {
        to: Uuid,
        payload: serde_json::Value,
    },
    Chat {
        text: String,
    },
    Leave,
}

#[derive(Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum ServerWsEvent {
    Welcome {
        room: RoomSnapshot,
        you: Uuid,
    },
    PeerJoined {
        peer: Participant,
    },
    PeerLeft {
        peer_id: Uuid,
    },
    Signal {
        from: Uuid,
        payload: serde_json::Value,
    },
    Chat {
        from: Uuid,
        display_name: String,
        text: String,
        at: DateTime<Utc>,
    },
    RoomUpdated {
        room: RoomSnapshot,
    },
    Error {
        message: String,
    },
}

#[derive(Clone, Serialize)]
struct Participant {
    session_id: Uuid,
    display_name: String,
}

#[derive(Clone, Serialize)]
struct RoomSnapshot {
    id: Uuid,
    host_session: Uuid,
    host_controls_joiners: bool,
    share_link_enabled: bool,
    participants: Vec<Participant>,
    waiting_for_random: bool,
    created_at: DateTime<Utc>,
    max_size: usize,
}

#[derive(Serialize)]
struct AuthResponse {
    session: Session,
}

#[derive(Serialize)]
struct OkResponse {
    ok: bool,
}

#[derive(Serialize)]
struct RoomResponse {
    room: RoomSnapshot,
    joined_existing: bool,
}

#[derive(Serialize)]
struct DirectoryResponse {
    entries: Vec<DirectoryEntry>,
}

#[derive(Serialize)]
struct JoinRequestsResponse {
    incoming: Vec<JoinRequestSummary>,
    outgoing: Vec<JoinRequestSummary>,
}

#[derive(Serialize)]
struct JoinRequestResponse {
    request: JoinRequestSummary,
}

#[derive(Clone, Serialize)]
struct JoinRequestSummary {
    id: Uuid,
    room_id: Uuid,
    requester_session: Uuid,
    requester_display_name: String,
    status: JoinRequestStatus,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    accepted_by: Option<Uuid>,
    can_accept: bool,
    room_is_full: bool,
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
    service: &'static str,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?),
        )
        .init();

    let app = build_router();
    let addr: SocketAddr = bind_addr()?.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "Camera Fodder listening");
    axum::serve(listener, app).await?;
    Ok(())
}

fn build_router() -> Router {
    let auth_store = AuthStore::from_env();
    let auth_data = auth_store.load().unwrap_or_else(|error| {
        tracing::warn!(%error, "Unable to load auth store; starting with empty auth data");
        AuthData::default()
    });
    let app = App {
        state: Arc::new(Mutex::new(AppState {
            users: auth_data.users,
            sessions: auth_data.sessions,
            ..AppState::default()
        })),
        auth_store,
    };
    build_router_with_app(app)
}

fn build_router_with_app(app: App) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/auth/guest", post(guest))
        .route("/api/auth/signup", post(signup))
        .route("/api/auth/signin", post(signin))
        .route("/api/auth/signout", post(signout))
        .route("/api/auth/session/:session_id", get(get_session))
        .route("/api/rooms/random", post(start_random_room))
        .route("/api/rooms/:room_id/add-random", post(add_random_to_room))
        .route("/api/rooms/:room_id/requests", post(create_join_request))
        .route("/api/join-requests", get(list_join_requests))
        .route(
            "/api/join-requests/:request_id/accept",
            post(accept_join_request),
        )
        .route("/api/directory", get(list_directory).post(upsert_directory))
        .route("/ws/rooms/:room_id", get(room_ws))
        .route("/room/:room_id", get(index_html))
        .nest_service(
            "/",
            ServeDir::new("public").append_index_html_on_directories(true),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(app)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        service: "camera-fodder",
    })
}

async fn index_html() -> Html<&'static str> {
    Html(include_str!("../public/index.html"))
}

async fn guest(
    State(app): State<App>,
    Json(payload): Json<GuestRequest>,
) -> Result<Json<AuthResponse>, ApiError> {
    let display_name = clean_name(payload.display_name.unwrap_or_else(|| "Guest".into()));
    let session = create_session(&app, display_name, None).await?;
    Ok(Json(AuthResponse { session }))
}

async fn signup(
    State(app): State<App>,
    Json(payload): Json<SignupRequest>,
) -> Result<Json<AuthResponse>, ApiError> {
    let email = payload.email.trim().to_ascii_lowercase();
    if email.is_empty() || payload.password.len() < 8 || payload.display_name.trim().is_empty() {
        return Err(ApiError::bad_request(
            "Email, display name, and an 8+ character password are required.",
        ));
    }
    let mut state = app.state.lock().await;
    if state.users.contains_key(&email) {
        return Err(ApiError::conflict("That email is already registered."));
    }
    let display_name = clean_name(payload.display_name);
    state.users.insert(
        email.clone(),
        UserAccount {
            display_name: display_name.clone(),
            password_hash: hash_password(&payload.password)?,
            created_at: Utc::now(),
        },
    );
    let session = new_session(display_name, Some(email));
    state.sessions.insert(session.id, session.clone());
    app.auth_store.save(&state)?;
    Ok(Json(AuthResponse { session }))
}

async fn signin(
    State(app): State<App>,
    Json(payload): Json<SigninRequest>,
) -> Result<Json<AuthResponse>, ApiError> {
    let email = payload.email.trim().to_ascii_lowercase();
    let mut state = app.state.lock().await;
    let account = state
        .users
        .get(&email)
        .ok_or_else(|| ApiError::unauthorized("Invalid email or password."))?;
    if !verify_password(&account.password_hash, &payload.password) {
        return Err(ApiError::unauthorized("Invalid email or password."));
    }
    let session = new_session(account.display_name.clone(), Some(email));
    state.sessions.insert(session.id, session.clone());
    app.auth_store.save(&state)?;
    Ok(Json(AuthResponse { session }))
}

async fn get_session(
    State(app): State<App>,
    Path(session_id): Path<Uuid>,
) -> Result<Json<AuthResponse>, ApiError> {
    let state = app.state.lock().await;
    let session = state
        .sessions
        .get(&session_id)
        .cloned()
        .ok_or_else(|| ApiError::unauthorized("Session expired. Please sign in again."))?;
    Ok(Json(AuthResponse { session }))
}

async fn signout(
    State(app): State<App>,
    Json(payload): Json<SignoutRequest>,
) -> Result<Json<OkResponse>, ApiError> {
    let mut state = app.state.lock().await;
    state.sessions.remove(&payload.session_id);
    state.directory.remove(&payload.session_id);
    app.auth_store.save(&state)?;
    Ok(Json(OkResponse { ok: true }))
}

async fn start_random_room(
    State(app): State<App>,
    Json(payload): Json<StartRoomRequest>,
) -> Result<Json<RoomResponse>, ApiError> {
    let requested_size = payload.size.unwrap_or(2).clamp(2, MAX_ROOM_SIZE);
    let host_controls = payload.host_controls_joiners.unwrap_or(false);
    let share_link_enabled = payload.share_link_enabled.unwrap_or(false);
    let mut state = app.state.lock().await;
    ensure_session(&state, payload.session_id)?;

    if let Some(room_id) = find_match(&state, requested_size, host_controls) {
        let snapshot = join_room_locked(&mut state, room_id, payload.session_id)?;
        return Ok(Json(RoomResponse {
            room: snapshot,
            joined_existing: true,
        }));
    }

    let room_id = Uuid::new_v4();
    let room = Room {
        id: room_id,
        host_session: payload.session_id,
        host_controls_joiners: host_controls,
        share_link_enabled,
        participants: HashSet::from([payload.session_id]),
        approved_joiners: HashSet::new(),
        senders: HashMap::new(),
        waiting_for_random: requested_size > 1,
        created_at: Utc::now(),
    };
    state.rooms.insert(room_id, room);
    let snapshot = room_snapshot(&state, room_id)?;
    Ok(Json(RoomResponse {
        room: snapshot,
        joined_existing: false,
    }))
}

async fn add_random_to_room(
    State(app): State<App>,
    Path(room_id): Path<Uuid>,
    Json(payload): Json<AddRandomRequest>,
) -> Result<Json<RoomResponse>, ApiError> {
    if room_id != payload.room_id {
        return Err(ApiError::bad_request(
            "Room URL and request body room_id do not match.",
        ));
    }
    let mut state = app.state.lock().await;
    let room = state
        .rooms
        .get_mut(&room_id)
        .ok_or_else(|| ApiError::not_found("Room not found."))?;
    if !room.participants.contains(&payload.session_id) {
        return Err(ApiError::unauthorized(
            "Join the room before inviting random people.",
        ));
    }
    if room.host_controls_joiners && room.host_session != payload.session_id {
        return Err(ApiError::forbidden(
            "The host limited add-person control for this room.",
        ));
    }
    if reserved_room_count(room) >= MAX_ROOM_SIZE {
        return Err(ApiError::conflict("This room is already full."));
    }
    room.waiting_for_random = true;
    let snapshot = room_snapshot(&state, room_id)?;
    broadcast(
        &state,
        room_id,
        ServerWsEvent::RoomUpdated {
            room: snapshot.clone(),
        },
    );
    Ok(Json(RoomResponse {
        room: snapshot,
        joined_existing: false,
    }))
}

async fn create_join_request(
    State(app): State<App>,
    Path(room_id): Path<Uuid>,
    Json(payload): Json<JoinRoomRequest>,
) -> Result<Json<JoinRequestResponse>, ApiError> {
    let mut state = app.state.lock().await;
    ensure_session(&state, payload.session_id)?;
    let requester_display_name = state
        .sessions
        .get(&payload.session_id)
        .map(|session| session.display_name.clone())
        .unwrap_or_else(|| "Guest".into());
    let room = state
        .rooms
        .get(&room_id)
        .ok_or_else(|| ApiError::not_found("Room not found."))?;
    if room.participants.contains(&payload.session_id) {
        return Err(ApiError::conflict("You are already in that room."));
    }
    if reserved_room_count(room) >= MAX_ROOM_SIZE {
        return Err(ApiError::conflict("That room is already full."));
    }

    if let Some(existing) = state.join_requests.values().find(|request| {
        request.room_id == room_id && request.requester_session == payload.session_id
    }) {
        return Ok(Json(JoinRequestResponse {
            request: join_request_summary(&state, existing, payload.session_id),
        }));
    }

    let now = Utc::now();
    let request = JoinRequest {
        id: Uuid::new_v4(),
        room_id,
        requester_session: payload.session_id,
        requester_display_name,
        status: JoinRequestStatus::Pending,
        created_at: now,
        updated_at: now,
        accepted_by: None,
    };
    let request_id = request.id;
    state.join_requests.insert(request_id, request);
    let request = state.join_requests.get(&request_id).unwrap();
    Ok(Json(JoinRequestResponse {
        request: join_request_summary(&state, request, payload.session_id),
    }))
}

async fn list_join_requests(
    State(app): State<App>,
    Query(query): Query<JoinRequestsQuery>,
) -> Result<Json<JoinRequestsResponse>, ApiError> {
    let state = app.state.lock().await;
    ensure_session(&state, query.session_id)?;

    let mut incoming: Vec<_> = state
        .join_requests
        .values()
        .filter(|request| request.status == JoinRequestStatus::Pending)
        .filter(|request| {
            state
                .rooms
                .get(&request.room_id)
                .is_some_and(|room| room.participants.contains(&query.session_id))
        })
        .map(|request| join_request_summary(&state, request, query.session_id))
        .collect();
    incoming.sort_by(|a, b| a.created_at.cmp(&b.created_at));

    let mut outgoing: Vec<_> = state
        .join_requests
        .values()
        .filter(|request| request.requester_session == query.session_id)
        .map(|request| join_request_summary(&state, request, query.session_id))
        .collect();
    outgoing.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));

    Ok(Json(JoinRequestsResponse { incoming, outgoing }))
}

async fn accept_join_request(
    State(app): State<App>,
    Path(request_id): Path<Uuid>,
    Json(payload): Json<JoinRoomRequest>,
) -> Result<Json<JoinRequestResponse>, ApiError> {
    let mut state = app.state.lock().await;
    ensure_session(&state, payload.session_id)?;

    let request = state
        .join_requests
        .get(&request_id)
        .ok_or_else(|| ApiError::not_found("Join request not found."))?
        .clone();
    let room = state
        .rooms
        .get(&request.room_id)
        .ok_or_else(|| ApiError::not_found("Room not found."))?;
    if !can_accept_join_request(room, payload.session_id) {
        return Err(ApiError::forbidden(
            "You do not have permission to accept requests for this room.",
        ));
    }
    if room.participants.contains(&request.requester_session) {
        return Err(ApiError::conflict("That person is already in the room."));
    }
    if !room.approved_joiners.contains(&request.requester_session)
        && reserved_room_count(room) >= MAX_ROOM_SIZE
    {
        return Err(ApiError::conflict("This room is already full."));
    }

    {
        let request = state.join_requests.get_mut(&request_id).unwrap();
        request.status = JoinRequestStatus::Accepted;
        request.updated_at = Utc::now();
        request.accepted_by = Some(payload.session_id);
    }
    {
        let room = state.rooms.get_mut(&request.room_id).unwrap();
        room.approved_joiners.insert(request.requester_session);
    }

    let request = state.join_requests.get(&request_id).unwrap();
    Ok(Json(JoinRequestResponse {
        request: join_request_summary(&state, request, payload.session_id),
    }))
}

async fn list_directory(State(app): State<App>) -> Json<DirectoryResponse> {
    let state = app.state.lock().await;
    let mut entries: Vec<_> = state
        .directory
        .values()
        .filter(|e| e.available)
        .cloned()
        .collect();
    entries.sort_by(|a, b| a.display_name.cmp(&b.display_name));
    Json(DirectoryResponse { entries })
}

async fn upsert_directory(
    State(app): State<App>,
    Json(payload): Json<DirectoryRequest>,
) -> Result<Json<DirectoryResponse>, ApiError> {
    let mut state = app.state.lock().await;
    ensure_session(&state, payload.session_id)?;
    if payload.available {
        state.directory.insert(
            payload.session_id,
            DirectoryEntry {
                session_id: payload.session_id,
                display_name: clean_name(payload.display_name),
                room_id: payload.room_id,
                available: true,
                updated_at: Utc::now(),
            },
        );
    } else {
        state.directory.remove(&payload.session_id);
    }
    let mut entries: Vec<_> = state
        .directory
        .values()
        .filter(|e| e.available)
        .cloned()
        .collect();
    entries.sort_by(|a, b| a.display_name.cmp(&b.display_name));
    Ok(Json(DirectoryResponse { entries }))
}

async fn room_ws(
    State(app): State<App>,
    Path(room_id): Path<Uuid>,
    Query(params): Query<WsParams>,
    ws: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    {
        let mut state = app.state.lock().await;
        ensure_session(&state, params.session_id)?;
        let room = state
            .rooms
            .get_mut(&room_id)
            .ok_or_else(|| ApiError::not_found("Room not found."))?;
        let is_new_joiner = !room.participants.contains(&params.session_id);
        let has_join_approval = room.approved_joiners.contains(&params.session_id);
        if is_new_joiner && !room.share_link_enabled && !has_join_approval {
            return Err(ApiError::forbidden(
                "This room requires a share link or an accepted join request.",
            ));
        }
        if is_new_joiner && room.participants.len() >= MAX_ROOM_SIZE {
            return Err(ApiError::conflict("Room is full."));
        }
        if is_new_joiner && !has_join_approval && reserved_room_count(room) >= MAX_ROOM_SIZE {
            return Err(ApiError::conflict("Room is full."));
        }
        room.participants.insert(params.session_id);
    }
    Ok(ws.on_upgrade(move |socket| handle_socket(app.state, room_id, params.session_id, socket)))
}

async fn handle_socket(state: SharedState, room_id: Uuid, session_id: Uuid, socket: WebSocket) {
    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<ServerWsEvent>();
    let mut display_name = "Guest".to_string();

    {
        let mut locked = state.lock().await;
        if let Some(session) = locked.sessions.get(&session_id) {
            display_name = session.display_name.clone();
        }
        if let Some(room) = locked.rooms.get_mut(&room_id) {
            room.senders.insert(session_id, tx.clone());
        }
        if let Ok(snapshot) = room_snapshot(&locked, room_id) {
            let _ = tx.send(ServerWsEvent::Welcome {
                room: snapshot,
                you: session_id,
            });
            broadcast_except(
                &locked,
                room_id,
                session_id,
                ServerWsEvent::PeerJoined {
                    peer: Participant {
                        session_id,
                        display_name: display_name.clone(),
                    },
                },
            );
        }
    }

    let send_task = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let Ok(text) = serde_json::to_string(&event) else {
                break;
            };
            if sender.send(Message::Text(text)).await.is_err() {
                break;
            }
        }
    });

    while let Some(Ok(message)) = receiver.next().await {
        let Message::Text(text) = message else {
            continue;
        };
        match serde_json::from_str::<ClientWsEvent>(&text) {
            Ok(ClientWsEvent::Signal { to, payload }) => {
                let locked = state.lock().await;
                send_to(
                    &locked,
                    room_id,
                    to,
                    ServerWsEvent::Signal {
                        from: session_id,
                        payload,
                    },
                );
            }
            Ok(ClientWsEvent::Chat { text }) => {
                let clipped: String = text.chars().take(500).collect();
                let locked = state.lock().await;
                broadcast(
                    &locked,
                    room_id,
                    ServerWsEvent::Chat {
                        from: session_id,
                        display_name: display_name.clone(),
                        text: clipped,
                        at: Utc::now(),
                    },
                );
            }
            Ok(ClientWsEvent::Leave) => break,
            Err(_) => {
                let locked = state.lock().await;
                send_to(
                    &locked,
                    room_id,
                    session_id,
                    ServerWsEvent::Error {
                        message: "Malformed websocket event.".into(),
                    },
                );
            }
        }
    }

    send_task.abort();
    let mut locked = state.lock().await;
    if let Some(room) = locked.rooms.get_mut(&room_id) {
        room.participants.remove(&session_id);
        room.senders.remove(&session_id);
        if room.host_session == session_id {
            if let Some(next_host) = room
                .participants
                .iter()
                .choose(&mut rand::thread_rng())
                .copied()
            {
                room.host_session = next_host;
            }
        }
        broadcast(
            room_to_state_ref(&locked),
            room_id,
            ServerWsEvent::PeerLeft {
                peer_id: session_id,
            },
        );
    }
    locked.directory.remove(&session_id);
    let remove_room = locked
        .rooms
        .get(&room_id)
        .is_some_and(|room| room.participants.is_empty());
    if remove_room {
        locked.rooms.remove(&room_id);
        locked
            .join_requests
            .retain(|_, request| request.room_id != room_id);
    } else if let Ok(snapshot) = room_snapshot(&locked, room_id) {
        broadcast(
            &locked,
            room_id,
            ServerWsEvent::RoomUpdated { room: snapshot },
        );
    }
}

fn room_to_state_ref(state: &AppState) -> &AppState {
    state
}

fn find_match(
    state: &AppState,
    requested_size: usize,
    requester_host_controls: bool,
) -> Option<Uuid> {
    state
        .rooms
        .values()
        .filter(|room| room.waiting_for_random)
        .filter(|room| reserved_room_count(room) < requested_size.min(MAX_ROOM_SIZE))
        .filter(|room| !(room.host_controls_joiners && requester_host_controls))
        .map(|room| room.id)
        .choose(&mut rand::thread_rng())
}

fn join_room_locked(
    state: &mut AppState,
    room_id: Uuid,
    session_id: Uuid,
) -> Result<RoomSnapshot, ApiError> {
    let room = state
        .rooms
        .get_mut(&room_id)
        .ok_or_else(|| ApiError::not_found("Room not found."))?;
    if reserved_room_count(room) >= MAX_ROOM_SIZE {
        return Err(ApiError::conflict("This room is full."));
    }
    room.participants.insert(session_id);
    if room.participants.len() >= 2 {
        room.waiting_for_random = false;
    }
    let snapshot = room_snapshot(state, room_id)?;
    broadcast(
        state,
        room_id,
        ServerWsEvent::RoomUpdated {
            room: snapshot.clone(),
        },
    );
    Ok(snapshot)
}

fn can_accept_join_request(room: &Room, session_id: Uuid) -> bool {
    room.participants.contains(&session_id)
        && (!room.host_controls_joiners || room.host_session == session_id)
}

fn reserved_room_count(room: &Room) -> usize {
    room.participants.len()
        + room
            .approved_joiners
            .iter()
            .filter(|session_id| !room.participants.contains(session_id))
            .count()
}

fn join_request_summary(
    state: &AppState,
    request: &JoinRequest,
    viewer_session: Uuid,
) -> JoinRequestSummary {
    let (can_accept, room_is_full) = state
        .rooms
        .get(&request.room_id)
        .map(|room| {
            let reserved_count = reserved_room_count(room);
            (
                request.status == JoinRequestStatus::Pending
                    && can_accept_join_request(room, viewer_session)
                    && reserved_count < MAX_ROOM_SIZE,
                reserved_count >= MAX_ROOM_SIZE,
            )
        })
        .unwrap_or((false, true));
    JoinRequestSummary {
        id: request.id,
        room_id: request.room_id,
        requester_session: request.requester_session,
        requester_display_name: request.requester_display_name.clone(),
        status: request.status,
        created_at: request.created_at,
        updated_at: request.updated_at,
        accepted_by: request.accepted_by,
        can_accept,
        room_is_full,
    }
}

fn room_snapshot(state: &AppState, room_id: Uuid) -> Result<RoomSnapshot, ApiError> {
    let room = state
        .rooms
        .get(&room_id)
        .ok_or_else(|| ApiError::not_found("Room not found."))?;
    let mut participants: Vec<_> = room
        .participants
        .iter()
        .filter_map(|id| {
            state.sessions.get(id).map(|session| Participant {
                session_id: *id,
                display_name: session.display_name.clone(),
            })
        })
        .collect();
    participants.sort_by(|a, b| a.display_name.cmp(&b.display_name));
    Ok(RoomSnapshot {
        id: room.id,
        host_session: room.host_session,
        host_controls_joiners: room.host_controls_joiners,
        share_link_enabled: room.share_link_enabled,
        participants,
        waiting_for_random: room.waiting_for_random,
        created_at: room.created_at,
        max_size: MAX_ROOM_SIZE,
    })
}

fn broadcast(state: &AppState, room_id: Uuid, event: ServerWsEvent) {
    if let Some(room) = state.rooms.get(&room_id) {
        for tx in room.senders.values() {
            let _ = tx.send(event.clone());
        }
    }
}

fn broadcast_except(state: &AppState, room_id: Uuid, except: Uuid, event: ServerWsEvent) {
    if let Some(room) = state.rooms.get(&room_id) {
        for (id, tx) in &room.senders {
            if *id != except {
                let _ = tx.send(event.clone());
            }
        }
    }
}

fn send_to(state: &AppState, room_id: Uuid, to: Uuid, event: ServerWsEvent) {
    if let Some(room) = state.rooms.get(&room_id) {
        if let Some(tx) = room.senders.get(&to) {
            let _ = tx.send(event);
        }
    }
}

async fn create_session(
    app: &App,
    display_name: String,
    account_email: Option<String>,
) -> Result<Session, ApiError> {
    let session = new_session(display_name, account_email);
    let mut state = app.state.lock().await;
    state.sessions.insert(session.id, session.clone());
    app.auth_store.save(&state)?;
    Ok(session)
}

fn new_session(display_name: String, account_email: Option<String>) -> Session {
    Session {
        id: Uuid::new_v4(),
        display_name,
        account_email,
        created_at: Utc::now(),
    }
}

fn ensure_session(state: &AppState, session_id: Uuid) -> Result<(), ApiError> {
    state
        .sessions
        .contains_key(&session_id)
        .then_some(())
        .ok_or_else(|| ApiError::unauthorized("A valid session is required."))
}

fn clean_name(name: String) -> String {
    let trimmed: String = name.trim().chars().take(32).collect();
    if trimmed.is_empty() {
        format!("Guest-{:04}", rand::thread_rng().next_u32() % 10_000)
    } else {
        trimmed
    }
}

fn hash_password(password: &str) -> Result<String, ApiError> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|_| ApiError::internal("Unable to hash password."))
}

fn verify_password(hash: &str, password: &str) -> bool {
    let Ok(parsed_hash) = PasswordHash::new(hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok()
}

fn bind_addr() -> anyhow::Result<String> {
    if let Ok(addr) = std::env::var("BIND_ADDR") {
        return Ok(addr);
    }
    if let Ok(port) = std::env::var("PORT") {
        return Ok(format!("0.0.0.0:{port}"));
    }
    Ok("0.0.0.0:3000".to_string())
}

impl AuthStore {
    fn from_env() -> Self {
        let path = std::env::var("AUTH_STORE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("data/auth.json"));
        Self { path: Some(path) }
    }

    fn load(&self) -> anyhow::Result<AuthData> {
        let Some(path) = &self.path else {
            return Ok(AuthData::default());
        };
        if !path.exists() {
            return Ok(AuthData::default());
        }
        let text = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&text)?)
    }

    fn save(&self, state: &AppState) -> Result<(), ApiError> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .map_err(|_| ApiError::internal("Unable to prepare auth store."))?;
        }
        let data = AuthData {
            users: state.users.clone(),
            sessions: state.sessions.clone(),
        };
        let text = serde_json::to_string_pretty(&data)
            .map_err(|_| ApiError::internal("Unable to serialize auth store."))?;
        let temp_path = path.with_extension("json.tmp");
        fs::write(&temp_path, text)
            .map_err(|_| ApiError::internal("Unable to write auth store."))?;
        fs::rename(&temp_path, path)
            .map_err(|_| ApiError::internal("Unable to save auth store."))?;
        Ok(())
    }
}

#[cfg(test)]
impl AuthStore {
    fn memory() -> Self {
        Self { path: None }
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: &str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
    fn unauthorized(message: &str) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }
    fn forbidden(message: &str) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }
    fn not_found(message: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
    fn conflict(message: &str) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
        }
    }
    fn internal(message: &str) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({ "error": self.message })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn test_app() -> App {
        App {
            state: Arc::new(Mutex::new(AppState::default())),
            auth_store: AuthStore::memory(),
        }
    }

    fn test_router() -> Router {
        build_router_with_app(test_app())
    }

    #[tokio::test]
    async fn health_endpoint_works() {
        let response = test_router()
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn room_page_serves_app_shell() {
        let response = test_router()
            .oneshot(
                Request::builder()
                    .uri(format!("/room/{}", Uuid::new_v4()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn host_control_rooms_do_not_match_each_other() {
        let app = test_app();
        let s1 = create_session(&app, "One".into(), None).await.unwrap();
        let s2 = create_session(&app, "Two".into(), None).await.unwrap();
        {
            let mut locked = app.state.lock().await;
            let room_id = Uuid::new_v4();
            locked.rooms.insert(
                room_id,
                Room {
                    id: room_id,
                    host_session: s1.id,
                    host_controls_joiners: true,
                    share_link_enabled: false,
                    participants: HashSet::from([s1.id]),
                    approved_joiners: HashSet::new(),
                    senders: HashMap::new(),
                    waiting_for_random: true,
                    created_at: Utc::now(),
                },
            );
            assert!(find_match(&locked, 2, true).is_none());
            assert!(find_match(&locked, 2, false).is_some());
        }
        assert_ne!(s1.id, s2.id);
    }

    #[tokio::test]
    async fn signup_signin_and_session_lookup_work() {
        let router = test_router();
        let signup_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/signup")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "email": "person@example.com",
                            "password": "correct horse battery staple",
                            "display_name": "Person"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(signup_response.status(), StatusCode::OK);
        let body = to_bytes(signup_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let session_id = payload["session"]["id"].as_str().unwrap();

        let signin_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/signin")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "email": "person@example.com",
                            "password": "correct horse battery staple"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(signin_response.status(), StatusCode::OK);

        let lookup_response = router
            .oneshot(
                Request::builder()
                    .uri(format!("/api/auth/session/{session_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(lookup_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn accepted_directory_request_approves_join_without_share_link() {
        let app = test_app();
        let router = build_router_with_app(app.clone());
        let host = create_session(&app, "Host".into(), None).await.unwrap();
        let requester = create_session(&app, "Joiner".into(), None).await.unwrap();
        let room_id = Uuid::new_v4();
        {
            let mut locked = app.state.lock().await;
            locked.rooms.insert(
                room_id,
                Room {
                    id: room_id,
                    host_session: host.id,
                    host_controls_joiners: false,
                    share_link_enabled: false,
                    participants: HashSet::from([host.id]),
                    approved_joiners: HashSet::new(),
                    senders: HashMap::new(),
                    waiting_for_random: false,
                    created_at: Utc::now(),
                },
            );
        }

        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/rooms/{room_id}/requests"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "session_id": requester.id }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let request_id = payload["request"]["id"].as_str().unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/join-requests/{request_id}/accept"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "session_id": host.id }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let locked = app.state.lock().await;
        let room = locked.rooms.get(&room_id).unwrap();
        assert!(room.approved_joiners.contains(&requester.id));
        assert!(!room.participants.contains(&requester.id));
    }
}
