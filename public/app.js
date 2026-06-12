const $ = (id) => document.getElementById(id);
const state = {
  session: JSON.parse(localStorage.getItem('cameraFodderSession') || 'null'),
  room: null,
  ws: null,
  localStream: null,
  peers: new Map(),
  participants: new Map(),
  roomEpoch: 0,
  joinRequests: { incoming: [], outgoing: [] },
  pendingRoomId: roomIdFromLocation(),
};

const rtcConfig = { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] };

function roomIdFromLocation() {
  const [, section, roomId] = location.pathname.match(/^\/(room)\/([^/?#]+)/) || [];
  return section === 'room' ? roomId : new URLSearchParams(location.search).get('room');
}

function roomPath(roomId) {
  return `/room/${roomId}`;
}

function showLobbyView() {
  document.body.classList.remove('in-room', 'in-invite');
  $('lobbyView').classList.remove('hidden');
  $('inviteView').classList.add('hidden');
  $('roomPanel').classList.add('hidden');
}

function showInviteView(roomId, replace = false) {
  state.pendingRoomId = roomId;
  document.body.classList.remove('in-room');
  document.body.classList.add('in-invite');
  $('lobbyView').classList.add('hidden');
  $('inviteView').classList.remove('hidden');
  $('roomPanel').classList.add('hidden');
  const nextPath = roomPath(roomId);
  if (location.pathname !== nextPath) {
    history[replace ? 'replaceState' : 'pushState']({}, '', nextPath);
  }
  renderInviteState();
}

function showRoomView(roomId, replace = false) {
  document.body.classList.remove('in-invite');
  document.body.classList.add('in-room');
  $('lobbyView').classList.add('hidden');
  $('inviteView').classList.add('hidden');
  $('roomPanel').classList.remove('hidden');
  const nextPath = roomPath(roomId);
  if (location.pathname !== nextPath) {
    history[replace ? 'replaceState' : 'pushState']({}, '', nextPath);
  }
}

function setSession(session) {
  state.session = session;
  localStorage.setItem('cameraFodderSession', JSON.stringify(session));
  $('connectionDot').classList.add('on');
  $('sessionName').textContent = session.display_name;
  $('sessionId').textContent = session.id;
  $('logoutButton').classList.remove('hidden');
  $('authStatus').textContent = `Signed in as ${session.display_name}.`;
  $('inviteConnectionDot').classList.add('on');
  $('inviteSessionName').textContent = session.display_name;
  $('inviteSessionId').textContent = session.id;
  $('inviteLogoutButton').classList.remove('hidden');
  refreshJoinRequests().catch(console.warn);
  if (state.pendingRoomId && !state.room) {
    renderInviteState();
  }
}

function clearSession(message = 'Use guest mode, or create/sign into a local account.') {
  state.session = null;
  localStorage.removeItem('cameraFodderSession');
  $('connectionDot').classList.remove('on');
  $('sessionName').textContent = 'Not signed in';
  $('sessionId').textContent = 'Use guest mode or local auth to begin.';
  $('logoutButton').classList.add('hidden');
  $('authStatus').textContent = message;
  $('inviteConnectionDot').classList.remove('on');
  $('inviteSessionName').textContent = 'Not signed in';
  $('inviteSessionId').textContent = 'Start a guest or local account session first.';
  $('inviteLogoutButton').classList.add('hidden');
  state.joinRequests = { incoming: [], outgoing: [] };
  renderJoinRequests();
}

async function validateStoredSession() {
  if (!state.session?.id) return;
  try {
    const { session } = await api(`/api/auth/session/${state.session.id}`, null, 'GET');
    setSession(session);
  } catch (error) {
    clearSession(error.message || 'Session expired. Please sign in again.');
  }
}

function requireSession() {
  if (!state.session?.id) throw new Error('Start a guest or local account session first.');
}

async function api(path, body, method = 'POST') {
  const response = await fetch(path, {
    method,
    headers: body ? { 'content-type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.error || response.statusText);
  return payload;
}

function isActiveRoom(roomId, roomEpoch) {
  return state.room?.id === roomId && state.roomEpoch === roomEpoch;
}

async function ensureMedia(roomEpoch = state.roomEpoch) {
  const roomId = state.room?.id;
  if (!roomId || !isActiveRoom(roomId, roomEpoch)) {
    throw new Error('No active room needs media.');
  }
  if (!state.localStream) {
    const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
    if (!isActiveRoom(roomId, roomEpoch)) {
      stream.getTracks().forEach((track) => track.stop());
      throw new Error('Room left before media started.');
    }
    state.localStream = stream;
    upsertVideo('local', state.localStream, `${state.session.display_name} (you)`, true);
  }
  return state.localStream;
}

function upsertVideo(id, stream, label, muted = false) {
  const card = ensureVideoCard(id, label);
  const video = card.querySelector('video');
  video.srcObject = stream;
  video.muted = muted;
  card.querySelector('.badge').textContent = label;
  card.querySelector('.video-state').textContent = '';
  card.classList.remove('waiting');
}

function ensureVideoCard(id, label) {
  let card = document.querySelector(`[data-video-id="${id}"]`);
  if (!card) {
    card = document.createElement('div');
    card.className = 'video-card waiting';
    card.dataset.videoId = id;
    card.innerHTML = '<video autoplay playsinline></video><span class="badge"></span><span class="video-state">Connecting...</span>';
    $('videos').append(card);
  }
  card.querySelector('.badge').textContent = label;
  return card;
}

function removeVideo(id) {
  document.querySelector(`[data-video-id="${id}"]`)?.remove();
}

function stopLocalMedia() {
  state.localStream?.getTracks().forEach((track) => track.stop());
  state.localStream = null;
  const localVideo = document.querySelector('[data-video-id="local"] video');
  if (localVideo) localVideo.srcObject = null;
}

function peerIsPolite(peerId) {
  return state.session.id > peerId;
}

function participantLabel(peerId) {
  return state.participants.get(peerId)?.display_name || `Peer ${peerId.slice(0, 8)}`;
}

async function createPeer(peerId, roomEpoch = state.roomEpoch) {
  const roomId = state.room?.id;
  if (!roomId || !isActiveRoom(roomId, roomEpoch)) return null;
  if (state.peers.has(peerId)) return state.peers.get(peerId);
  const pc = new RTCPeerConnection(rtcConfig);
  const record = { pc, makingOffer: false, ignoreOffer: false, polite: peerIsPolite(peerId) };
  state.peers.set(peerId, record);
  ensureVideoCard(peerId, participantLabel(peerId));
  let stream;
  try {
    stream = await ensureMedia(roomEpoch);
  } catch (error) {
    pc.close();
    state.peers.delete(peerId);
    removeVideo(peerId);
    if (isActiveRoom(roomId, roomEpoch)) console.warn(error);
    return null;
  }
  if (!isActiveRoom(roomId, roomEpoch)) {
    pc.close();
    state.peers.delete(peerId);
    removeVideo(peerId);
    return null;
  }
  stream.getTracks().forEach((track) => pc.addTrack(track, stream));
  pc.ontrack = (event) => upsertVideo(peerId, event.streams[0], participantLabel(peerId));
  pc.onicecandidate = ({ candidate }) => candidate && send({ type: 'signal', to: peerId, payload: { candidate } });
  pc.onnegotiationneeded = async () => {
    try {
      record.makingOffer = true;
      await pc.setLocalDescription();
      send({ type: 'signal', to: peerId, payload: { description: pc.localDescription } });
    } catch (error) {
      console.warn('Unable to negotiate peer connection', error);
    } finally {
      record.makingOffer = false;
    }
  };
  return record;
}

async function handleSignal(from, payload, roomEpoch = state.roomEpoch) {
  const roomId = state.room?.id;
  if (!roomId || !isActiveRoom(roomId, roomEpoch)) return;
  const record = await createPeer(from, roomEpoch);
  if (!record || !isActiveRoom(roomId, roomEpoch)) return;
  const { pc } = record;
  if (payload.description) {
    const offerCollision = payload.description.type === 'offer' && (record.makingOffer || pc.signalingState !== 'stable');
    record.ignoreOffer = !record.polite && offerCollision;
    if (record.ignoreOffer) return;
    await pc.setRemoteDescription(payload.description);
    if (payload.description.type === 'offer') {
      await pc.setLocalDescription();
      send({ type: 'signal', to: from, payload: { description: pc.localDescription } });
    }
  } else if (payload.candidate) {
    try {
      await pc.addIceCandidate(payload.candidate);
    } catch (error) {
      if (!record.ignoreOffer) console.warn('Unable to add ICE candidate', error);
    }
  }
}

function send(event) {
  if (state.ws?.readyState === WebSocket.OPEN) state.ws.send(JSON.stringify(event));
}

async function joinRoom(room, options = {}) {
  requireSession();
  const roomEpoch = ++state.roomEpoch;
  state.room = room;
  showRoomView(room.id, options.replaceUrl);
  $('addRandomButton').disabled = false;
  $('roomTitle').textContent = `Room ${room.id.slice(0, 8)}`;
  renderJoinRequests();
  if (room.participants) {
    rememberParticipants(room);
    renderRoomMeta(room);
  }
  else $('roomMeta').textContent = 'Joining shared room…';
  const wsUrl = `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws/rooms/${room.id}?session_id=${state.session.id}`;
  if (state.ws) state.ws.onclose = null;
  state.ws?.close();
  state.ws = new WebSocket(wsUrl);
  state.ws.onmessage = async ({ data }) => {
    if (!isActiveRoom(room.id, roomEpoch)) return;
    const event = JSON.parse(data);
    if (event.type === 'welcome') {
      if (!isActiveRoom(room.id, roomEpoch)) return;
      state.room = event.room;
      rememberParticipants(event.room);
      renderRoomMeta(event.room);
      refreshJoinRequests().catch(console.warn);
      try {
        await ensureMedia(roomEpoch);
      } catch (error) {
        if (isActiveRoom(room.id, roomEpoch)) console.warn(error);
        return;
      }
      if (!isActiveRoom(room.id, roomEpoch)) return;
      for (const peer of event.room.participants.filter((p) => p.session_id !== state.session.id)) {
        if (!isActiveRoom(room.id, roomEpoch)) return;
        await createPeer(peer.session_id, roomEpoch);
      }
    }
    if (event.type === 'peerJoined') {
      if (!isActiveRoom(room.id, roomEpoch)) return;
      state.participants.set(event.peer.session_id, event.peer);
      renderParticipants();
      await createPeer(event.peer.session_id, roomEpoch);
    }
    if (event.type === 'peerLeft') {
      if (!isActiveRoom(room.id, roomEpoch)) return;
      state.peers.get(event.peer_id)?.pc.close();
      state.peers.delete(event.peer_id);
      state.participants.delete(event.peer_id);
      renderParticipants();
      removeVideo(event.peer_id);
    }
    if (event.type === 'signal') await handleSignal(event.from, event.payload, roomEpoch);
    if (event.type === 'chat') appendMessage(`${event.display_name}: ${event.text}`);
    if (event.type === 'roomUpdated') {
      if (!isActiveRoom(room.id, roomEpoch)) return;
      state.room = event.room;
      rememberParticipants(event.room);
      renderRoomMeta(event.room);
    }
    if (event.type === 'error') appendMessage(`Server: ${event.message}`);
  };
  state.ws.onclose = () => appendMessage('Disconnected from room.');
}

function renderRoomMeta(room) {
  $('roomMeta').textContent = `${room.participants.length}/${room.max_size} people · ${room.waiting_for_random ? 'waiting for random match' : 'matched'} · ${room.host_controls_joiners ? 'host controls add-person' : 'anyone can add people'}`;
}

function rememberParticipants(room) {
  state.participants.clear();
  for (const participant of room.participants || []) {
    state.participants.set(participant.session_id, participant);
    const card = document.querySelector(`[data-video-id="${participant.session_id}"]`);
    if (card) card.querySelector('.badge').textContent = participant.session_id === state.session?.id
      ? `${participant.display_name} (you)`
      : participant.display_name;
  }
  renderParticipants();
}

function renderParticipants() {
  const list = $('participants');
  if (!list) return;
  list.innerHTML = '';
  const participants = [...state.participants.values()];
  if (!participants.length) {
    appendHint(list, 'No one is in the room yet.');
    return;
  }
  for (const participant of participants) {
    const item = document.createElement('div');
    item.className = 'participant';
    const name = document.createElement('strong');
    name.textContent = participant.session_id === state.session?.id
      ? `${participant.display_name} (you)`
      : participant.display_name;
    const status = document.createElement('small');
    status.textContent = participant.session_id === state.room?.host_session ? 'Host' : 'Guest';
    item.append(name, status);
    list.append(item);
  }
}

function appendMessage(text) {
  const line = document.createElement('div');
  line.textContent = text;
  $('messages').append(line);
  $('messages').scrollTop = $('messages').scrollHeight;
}

function formatRoomId(roomId) {
  return `Room ${roomId.slice(0, 8)}`;
}

function setRequestStatus(message) {
  const text = message || (state.session
    ? 'Requests are checked quietly in the background.'
    : 'Start a session to send and accept requests.');
  $('requestStatus').textContent = text;
  if ($('directoryStatus')) $('directoryStatus').textContent = text;
  if (message && $('inviteStatus')) $('inviteStatus').textContent = message;
}

function outgoingRequestForRoom(roomId) {
  return state.joinRequests.outgoing.find((request) => request.room_id === roomId);
}

function renderInviteState() {
  const roomId = state.pendingRoomId || roomIdFromLocation();
  if (!roomId || !$('inviteView')) return;
  $('inviteRoomTitle').textContent = `Request to join ${formatRoomId(roomId)}`;
  if (state.session) {
    $('inviteConnectionDot').classList.add('on');
    $('inviteSessionName').textContent = state.session.display_name;
    $('inviteSessionId').textContent = state.session.id;
  } else {
    $('inviteConnectionDot').classList.remove('on');
    $('inviteSessionName').textContent = 'Not signed in';
    $('inviteSessionId').textContent = 'Start a guest or local account session first.';
  }

  const existing = outgoingRequestForRoom(roomId);
  $('inviteJoinButton').classList.add('hidden');
  $('inviteRequestButton').classList.remove('hidden');

  if (!state.session) {
    $('inviteStatus').textContent = 'Choose a name, start a session, then send a request to the people already in this room.';
    $('inviteRequestButton').disabled = true;
    $('inviteRequestButton').textContent = 'Sign in to request';
  } else if (state.room?.id === roomId) {
    $('inviteStatus').textContent = 'You are already in this room.';
    $('inviteRequestButton').disabled = true;
    $('inviteRequestButton').textContent = 'Already in room';
  } else if (existing?.status === 'pending') {
    $('inviteStatus').textContent = 'Request sent. Waiting for someone in the room to accept it.';
    $('inviteRequestButton').disabled = true;
    $('inviteRequestButton').textContent = 'Request sent';
  } else if (existing?.status === 'accepted') {
    $('inviteStatus').textContent = 'Your request was accepted. You can join the room now.';
    $('inviteRequestButton').classList.add('hidden');
    $('inviteJoinButton').classList.remove('hidden');
    $('inviteJoinButton').disabled = false;
  } else {
    $('inviteStatus').textContent = `Signed in as ${state.session.display_name}. Send a request when you are ready.`;
    $('inviteRequestButton').disabled = false;
    $('inviteRequestButton').textContent = 'Request to join';
  }
}

async function refreshDirectory() {
  const { entries } = await api('/api/directory', null, 'GET');
  $('directory').innerHTML = '';
  if (!entries.length) {
    const empty = document.createElement('p');
    empty.className = 'hint';
    empty.textContent = 'Nobody is listed yet. Opt in to appear here while online.';
    $('directory').append(empty);
    return;
  }
  for (const entry of entries) {
    const card = document.createElement('div');
    card.className = 'person';
    const name = document.createElement('strong');
    name.textContent = entry.display_name;
    const status = document.createElement('small');
    status.textContent = entry.room_id ? `In ${formatRoomId(entry.room_id)}` : 'Available';
    const actions = document.createElement('div');
    actions.className = 'person-actions';

    if (entry.session_id === state.session?.id) {
      const self = document.createElement('small');
      self.textContent = 'This is you';
      actions.append(self);
    } else if (entry.room_id) {
      const existing = outgoingRequestForRoom(entry.room_id);
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'secondary';
      if (!state.session) {
        button.disabled = true;
        button.textContent = 'Start session to request';
      } else if (state.room?.id === entry.room_id) {
        button.disabled = true;
        button.textContent = 'Already in room';
      } else if (existing?.status === 'pending') {
        button.disabled = true;
        button.textContent = 'Requested';
      } else if (existing?.status === 'accepted') {
        button.textContent = 'Join room';
        button.onclick = () => joinAcceptedRoom(existing);
      } else {
        button.textContent = 'Request to join';
        button.onclick = () => sendJoinRequest(entry.room_id);
      }
      actions.append(button);
    }

    card.append(name, status);
    if (actions.children.length) card.append(actions);
    $('directory').append(card);
  }
}

async function refreshJoinRequests() {
  if (!state.session) {
    state.joinRequests = { incoming: [], outgoing: [] };
    renderJoinRequests();
    return;
  }
  const query = new URLSearchParams({ session_id: state.session.id });
  state.joinRequests = await api(`/api/join-requests?${query}`, null, 'GET');
  renderJoinRequests();
}

function renderJoinRequests() {
  renderIncomingRequests();
  renderOutgoingRequests();
  renderInviteState();
}

function renderIncomingRequests() {
  const list = $('incomingRequests');
  list.innerHTML = '';
  if (!state.session) {
    appendHint(list, 'Start a session to receive room requests.');
    return;
  }
  if (!state.room) {
    appendHint(list, 'Start or join a room to receive requests.');
    return;
  }
  if (!state.joinRequests.incoming.length) {
    appendHint(list, 'No pending requests.');
    return;
  }
  for (const request of state.joinRequests.incoming) {
    const card = requestCard(
      `${request.requester_display_name} wants to join`,
      `${formatRoomId(request.room_id)} · ${request.status}`
    );
    if (request.can_accept) {
      const button = document.createElement('button');
      button.type = 'button';
      button.textContent = 'Accept';
      button.onclick = () => acceptJoinRequest(request);
      card.append(button);
    } else {
      appendHint(card, request.room_is_full ? 'Room is full.' : 'Only the host can accept this request.');
    }
    list.append(card);
  }
}

function renderOutgoingRequests() {
  const list = $('outgoingRequests');
  list.innerHTML = '';
  if (!state.session) {
    appendHint(list, 'Start a session to request a room.');
    return;
  }
  if (!state.joinRequests.outgoing.length) {
    appendHint(list, 'No sent requests.');
    return;
  }
  for (const request of state.joinRequests.outgoing) {
    const joined = state.room?.id === request.room_id;
    const card = requestCard(
      formatRoomId(request.room_id),
      joined ? 'Joined' : request.status
    );
    if (request.status === 'accepted' && !joined) {
      const button = document.createElement('button');
      button.type = 'button';
      button.textContent = 'Join room';
      button.onclick = () => joinAcceptedRoom(request);
      card.append(button);
    }
    list.append(card);
  }
}

function requestCard(title, meta) {
  const card = document.createElement('div');
  card.className = 'request-card';
  const heading = document.createElement('strong');
  heading.textContent = title;
  const detail = document.createElement('span');
  detail.className = 'request-meta';
  detail.textContent = meta;
  card.append(heading, detail);
  return card;
}

function appendHint(parent, text) {
  const hint = document.createElement('p');
  hint.className = 'hint';
  hint.textContent = text;
  parent.append(hint);
}

async function sendJoinRequest(roomId) {
  try {
    requireSession();
    if (state.room?.id === roomId) {
      setRequestStatus('You are already in that room.');
      renderInviteState();
      return;
    }
    const { request } = await api(`/api/rooms/${roomId}/requests`, { session_id: state.session.id });
    setRequestStatus(request.status === 'accepted' ? 'Your request was already accepted.' : `Request sent to ${formatRoomId(roomId)}.`);
    await refreshJoinRequests();
    await refreshDirectory();
    renderInviteState();
  } catch (error) {
    setRequestStatus(error.message);
    renderInviteState();
  }
}

async function acceptJoinRequest(request) {
  try {
    const { request: accepted } = await api(`/api/join-requests/${request.id}/accept`, { session_id: state.session.id });
    setRequestStatus(`Accepted ${accepted.requester_display_name} for ${formatRoomId(accepted.room_id)}.`);
    await refreshJoinRequests();
    await refreshDirectory();
  } catch (error) {
    setRequestStatus(error.message);
  }
}

async function joinAcceptedRoom(request) {
  try {
    if (state.room && state.room.id !== request.room_id) await leaveCurrentRoom(false, false);
    state.pendingRoomId = null;
    await joinRoom({ id: request.room_id });
    await updateDirectory();
    await refreshJoinRequests();
  } catch (error) {
    setRequestStatus(error.message);
  }
}

async function updateDirectory() {
  if (!state.session) return;
  await api('/api/directory', {
    session_id: state.session.id,
    display_name: state.session.display_name,
    room_id: state.room?.id || null,
    available: $('directoryOptIn').checked,
  });
  await refreshDirectory();
}

async function leaveCurrentRoom(updateListing = true, navigateHome = true) {
  state.roomEpoch += 1;
  send({ type: 'leave' });
  if (state.ws) {
    state.ws.onmessage = null;
    state.ws.onclose = null;
    state.ws.onerror = null;
  }
  state.ws?.close();
  state.ws = null;
  state.peers.forEach(({ pc }) => pc.close());
  state.peers.clear();
  state.participants.clear();
  stopLocalMedia();
  state.room = null;
  $('videos').innerHTML = '';
  renderParticipants();
  showLobbyView();
  if (navigateHome && location.pathname !== '/') history.pushState({}, '', '/');
  $('addRandomButton').disabled = true;
  if (updateListing) await updateDirectory();
  state.pendingRoomId = roomIdFromLocation();
}

async function startGuest(displayNameId) {
  setSession((await api('/api/auth/guest', { display_name: $(displayNameId).value || null })).session);
}

async function signup(displayNameId, emailId, passwordId) {
  setSession((await api('/api/auth/signup', { email: $(emailId).value, password: $(passwordId).value, display_name: $(displayNameId).value })).session);
}

async function signin(emailId, passwordId) {
  setSession((await api('/api/auth/signin', { email: $(emailId).value, password: $(passwordId).value })).session);
}

async function runAuth(action, statusId = 'authStatus') {
  try {
    $(statusId).textContent = 'Working...';
    await action();
  } catch (error) {
    $(statusId).textContent = error.message;
    if (statusId !== 'authStatus') $('authStatus').textContent = error.message;
  }
}

async function signout() {
  const sessionId = state.session?.id;
  if (state.room) await leaveCurrentRoom(true, false);
  if (sessionId) await api('/api/auth/signout', { session_id: sessionId }).catch(console.warn);
  clearSession('Signed out.');
  renderInviteState();
}

$('guestButton').onclick = async () => runAuth(() => startGuest('displayName'));
$('signupButton').onclick = async () => runAuth(() => signup('displayName', 'email', 'password'));
$('signinButton').onclick = async () => runAuth(() => signin('email', 'password'));
$('inviteGuestButton').onclick = async () => runAuth(() => startGuest('inviteDisplayName'), 'inviteStatus');
$('inviteSignupButton').onclick = async () => runAuth(() => signup('inviteDisplayName', 'inviteEmail', 'invitePassword'), 'inviteStatus');
$('inviteSigninButton').onclick = async () => runAuth(() => signin('inviteEmail', 'invitePassword'), 'inviteStatus');
$('logoutButton').onclick = signout;
$('inviteLogoutButton').onclick = signout;
$('inviteRequestButton').onclick = async () => {
  const roomId = state.pendingRoomId || roomIdFromLocation();
  if (roomId) await sendJoinRequest(roomId);
};
$('inviteJoinButton').onclick = async () => {
  const roomId = state.pendingRoomId || roomIdFromLocation();
  const request = roomId ? outgoingRequestForRoom(roomId) : null;
  if (request?.status === 'accepted') await joinAcceptedRoom(request);
};
$('startRoomButton').onclick = async () => {
  try {
    requireSession();
    const { room } = await api('/api/rooms/random', {
      session_id: state.session.id,
      size: Number($('roomSize').value || 2),
      host_controls_joiners: $('hostControls').checked,
      share_link_enabled: $('shareLinks').checked,
    });
    await joinRoom(room);
    await updateDirectory();
    await refreshJoinRequests();
  } catch (error) { alert(error.message); }
};
$('addRandomButton').onclick = async () => {
  try {
    const { room } = await api(`/api/rooms/${state.room.id}/add-random`, { session_id: state.session.id, room_id: state.room.id });
    state.room = room;
    renderRoomMeta(room);
  } catch (error) { alert(error.message); }
};
$('copyLinkButton').onclick = async () => navigator.clipboard.writeText(`${location.origin}${roomPath(state.room.id)}`);
$('leaveButton').onclick = async () => {
  await leaveCurrentRoom();
  await refreshJoinRequests();
};
$('chatForm').onsubmit = (event) => {
  event.preventDefault();
  const text = $('chatInput').value.trim();
  if (text) send({ type: 'chat', text });
  $('chatInput').value = '';
};
$('directoryOptIn').onchange = updateDirectory;
window.addEventListener('beforeunload', () => {
  stopLocalMedia();
  if (state.session) navigator.sendBeacon('/api/directory', new Blob([JSON.stringify({ session_id: state.session.id, display_name: state.session.display_name, room_id: null, available: false })], { type: 'application/json' }));
});
window.addEventListener('popstate', () => {
  const roomId = roomIdFromLocation();
  if (roomId && state.room?.id === roomId) {
    showRoomView(roomId, true);
  } else if (roomId) {
    if (state.room) leaveCurrentRoom(true, false).catch(console.warn);
    showInviteView(roomId, true);
  } else if (!roomId && state.room) {
    leaveCurrentRoom(true, false).catch(console.warn);
  } else if (!roomId) {
    state.pendingRoomId = null;
    showLobbyView();
  }
});

if (state.pendingRoomId) showInviteView(state.pendingRoomId, true);
else showLobbyView();
setRequestStatus();
renderJoinRequests();
if (state.session) validateStoredSession().catch(console.warn);
refreshDirectory().catch(console.warn);
setInterval(() => {
  refreshDirectory().catch(console.warn);
  refreshJoinRequests().catch(console.warn);
}, 5000);
