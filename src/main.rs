use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rand::{seq::IteratorRandom, RngCore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
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
}

#[derive(Default)]
struct AppState {
    users: HashMap<String, UserAccount>,
    sessions: HashMap<Uuid, Session>,
    rooms: HashMap<Uuid, Room>,
    directory: HashMap<Uuid, DirectoryEntry>,
}

#[derive(Clone)]
struct UserAccount {
    display_name: String,
    password_hash: String,
}

#[derive(Clone, Serialize)]
struct Session {
    id: Uuid,
    display_name: String,
    account_email: Option<String>,
    created_at: DateTime<Utc>,
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
    senders: HashMap<Uuid, mpsc::UnboundedSender<ServerWsEvent>>,
    waiting_for_random: bool,
    created_at: DateTime<Utc>,
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
struct RoomResponse {
    room: RoomSnapshot,
    joined_existing: bool,
}

#[derive(Serialize)]
struct DirectoryResponse {
    entries: Vec<DirectoryEntry>,
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
    let addr: SocketAddr = std::env::var("BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:3000".to_string())
        .parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "Camera Fodder listening");
    axum::serve(listener, app).await?;
    Ok(())
}

fn build_router() -> Router {
    let app = App {
        state: Arc::new(Mutex::new(AppState::default())),
    };
    Router::new()
        .route("/api/health", get(health))
        .route("/api/auth/guest", post(guest))
        .route("/api/auth/signup", post(signup))
        .route("/api/auth/signin", post(signin))
        .route("/api/rooms/random", post(start_random_room))
        .route("/api/rooms/:room_id/add-random", post(add_random_to_room))
        .route("/api/directory", get(list_directory).post(upsert_directory))
        .route("/ws/rooms/:room_id", get(room_ws))
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

async fn guest(State(app): State<App>, Json(payload): Json<GuestRequest>) -> Json<AuthResponse> {
    let display_name = clean_name(payload.display_name.unwrap_or_else(|| "Guest".into()));
    Json(AuthResponse {
        session: create_session(&app.state, display_name, None).await,
    })
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
    state.users.insert(
        email.clone(),
        UserAccount {
            display_name: clean_name(payload.display_name),
            password_hash: hash_password(&payload.password),
        },
    );
    let account = state.users.get(&email).unwrap().clone();
    let session = new_session(account.display_name, Some(email));
    state.sessions.insert(session.id, session.clone());
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
    if account.password_hash != hash_password(&payload.password) {
        return Err(ApiError::unauthorized("Invalid email or password."));
    }
    let session = new_session(account.display_name.clone(), Some(email));
    state.sessions.insert(session.id, session.clone());
    Ok(Json(AuthResponse { session }))
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
    if room.participants.len() >= MAX_ROOM_SIZE {
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
        let is_new_share_joiner = !room.participants.contains(&params.session_id);
        if is_new_share_joiner && !room.share_link_enabled {
            return Err(ApiError::forbidden(
                "This room does not allow share-link joins.",
            ));
        }
        if is_new_share_joiner && room.participants.len() >= MAX_ROOM_SIZE {
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
            if let Some(next_host) = room.participants.iter().next().copied() {
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
        .filter(|room| room.participants.len() < requested_size.min(MAX_ROOM_SIZE))
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
    if room.participants.len() >= MAX_ROOM_SIZE {
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
    state: &SharedState,
    display_name: String,
    account_email: Option<String>,
) -> Session {
    let session = new_session(display_name, account_email);
    state
        .lock()
        .await
        .sessions
        .insert(session.id, session.clone());
    session
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

fn hash_password(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"camera-fodder-dev-salt");
    hasher.update(password.as_bytes());
    format!("{:x}", hasher.finalize())
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
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_endpoint_works() {
        let response = build_router()
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
    async fn host_control_rooms_do_not_match_each_other() {
        let state = Arc::new(Mutex::new(AppState::default()));
        let s1 = create_session(&state, "One".into(), None).await;
        let s2 = create_session(&state, "Two".into(), None).await;
        {
            let mut locked = state.lock().await;
            let room_id = Uuid::new_v4();
            locked.rooms.insert(
                room_id,
                Room {
                    id: room_id,
                    host_session: s1.id,
                    host_controls_joiners: true,
                    share_link_enabled: false,
                    participants: HashSet::from([s1.id]),
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
}
