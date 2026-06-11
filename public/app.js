const $ = (id) => document.getElementById(id);
const state = {
  session: JSON.parse(localStorage.getItem('cameraFodderSession') || 'null'),
  room: null,
  ws: null,
  localStream: null,
  peers: new Map(),
  pendingRoomId: new URLSearchParams(location.search).get('room'),
};

const rtcConfig = { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] };

function setSession(session) {
  state.session = session;
  localStorage.setItem('cameraFodderSession', JSON.stringify(session));
  $('connectionDot').classList.add('on');
  $('sessionName').textContent = session.display_name;
  $('sessionId').textContent = session.id;
  if (state.pendingRoomId && !state.room) {
    joinRoom({ id: state.pendingRoomId }).catch((error) => alert(error.message));
    state.pendingRoomId = null;
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

async function ensureMedia() {
  if (!state.localStream) {
    state.localStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
    upsertVideo('local', state.localStream, `${state.session.display_name} (you)`, true);
  }
  return state.localStream;
}

function upsertVideo(id, stream, label, muted = false) {
  let card = document.querySelector(`[data-video-id="${id}"]`);
  if (!card) {
    card = document.createElement('div');
    card.className = 'video-card';
    card.dataset.videoId = id;
    card.innerHTML = '<video autoplay playsinline></video><span class="badge"></span>';
    $('videos').append(card);
  }
  const video = card.querySelector('video');
  video.srcObject = stream;
  video.muted = muted;
  card.querySelector('.badge').textContent = label;
}

function removeVideo(id) {
  document.querySelector(`[data-video-id="${id}"]`)?.remove();
}

async function createPeer(peerId, polite = false) {
  if (state.peers.has(peerId)) return state.peers.get(peerId);
  const pc = new RTCPeerConnection(rtcConfig);
  const record = { pc, makingOffer: false, polite };
  state.peers.set(peerId, record);
  const stream = await ensureMedia();
  stream.getTracks().forEach((track) => pc.addTrack(track, stream));
  pc.ontrack = (event) => upsertVideo(peerId, event.streams[0], `Peer ${peerId.slice(0, 8)}`);
  pc.onicecandidate = ({ candidate }) => candidate && send({ type: 'signal', to: peerId, payload: { candidate } });
  pc.onnegotiationneeded = async () => {
    try {
      record.makingOffer = true;
      await pc.setLocalDescription();
      send({ type: 'signal', to: peerId, payload: { description: pc.localDescription } });
    } finally {
      record.makingOffer = false;
    }
  };
  return record;
}

async function handleSignal(from, payload) {
  const record = await createPeer(from, true);
  const { pc } = record;
  if (payload.description) {
    const offerCollision = payload.description.type === 'offer' && (record.makingOffer || pc.signalingState !== 'stable');
    if (offerCollision && !record.polite) return;
    await pc.setRemoteDescription(payload.description);
    if (payload.description.type === 'offer') {
      await pc.setLocalDescription();
      send({ type: 'signal', to: from, payload: { description: pc.localDescription } });
    }
  } else if (payload.candidate) {
    await pc.addIceCandidate(payload.candidate).catch(console.warn);
  }
}

function send(event) {
  if (state.ws?.readyState === WebSocket.OPEN) state.ws.send(JSON.stringify(event));
}

async function joinRoom(room) {
  requireSession();
  state.room = room;
  $('roomPanel').classList.remove('hidden');
  $('addRandomButton').disabled = false;
  $('roomTitle').textContent = `Room ${room.id.slice(0, 8)}`;
  if (room.participants) renderRoomMeta(room);
  else $('roomMeta').textContent = 'Joining shared room…';
  await ensureMedia();
  const wsUrl = `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws/rooms/${room.id}?session_id=${state.session.id}`;
  state.ws?.close();
  state.ws = new WebSocket(wsUrl);
  state.ws.onmessage = async ({ data }) => {
    const event = JSON.parse(data);
    if (event.type === 'welcome') {
      state.room = event.room;
      renderRoomMeta(event.room);
      for (const peer of event.room.participants.filter((p) => p.session_id !== state.session.id)) {
        await createPeer(peer.session_id, state.session.id > peer.session_id);
      }
    }
    if (event.type === 'peerJoined') await createPeer(event.peer.session_id, false);
    if (event.type === 'peerLeft') {
      state.peers.get(event.peer_id)?.pc.close();
      state.peers.delete(event.peer_id);
      removeVideo(event.peer_id);
    }
    if (event.type === 'signal') await handleSignal(event.from, event.payload);
    if (event.type === 'chat') appendMessage(`${event.display_name}: ${event.text}`);
    if (event.type === 'roomUpdated') { state.room = event.room; renderRoomMeta(event.room); }
    if (event.type === 'error') appendMessage(`Server: ${event.message}`);
  };
  state.ws.onclose = () => appendMessage('Disconnected from room.');
}

function renderRoomMeta(room) {
  $('roomMeta').textContent = `${room.participants.length}/${room.max_size} people · ${room.waiting_for_random ? 'waiting for random match' : 'matched'} · ${room.host_controls_joiners ? 'host controls add-person' : 'anyone can add people'}`;
}

function appendMessage(text) {
  const line = document.createElement('div');
  line.textContent = text;
  $('messages').append(line);
  $('messages').scrollTop = $('messages').scrollHeight;
}

async function refreshDirectory() {
  const { entries } = await api('/api/directory', null, 'GET');
  $('directory').innerHTML = entries.length ? '' : '<p class="hint">Nobody is listed yet. Opt in to appear here while online.</p>';
  for (const entry of entries) {
    const card = document.createElement('div');
    card.className = 'person';
    card.innerHTML = `<strong>${entry.display_name}</strong><small>${entry.room_id ? `In room ${entry.room_id.slice(0, 8)}` : 'Available'}</small>`;
    $('directory').append(card);
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

$('guestButton').onclick = async () => setSession((await api('/api/auth/guest', { display_name: $('displayName').value || null })).session);
$('signupButton').onclick = async () => setSession((await api('/api/auth/signup', { email: $('email').value, password: $('password').value, display_name: $('displayName').value })).session);
$('signinButton').onclick = async () => setSession((await api('/api/auth/signin', { email: $('email').value, password: $('password').value })).session);
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
  } catch (error) { alert(error.message); }
};
$('addRandomButton').onclick = async () => {
  try {
    const { room } = await api(`/api/rooms/${state.room.id}/add-random`, { session_id: state.session.id, room_id: state.room.id });
    state.room = room;
    renderRoomMeta(room);
  } catch (error) { alert(error.message); }
};
$('copyLinkButton').onclick = async () => navigator.clipboard.writeText(`${location.origin}/?room=${state.room.id}`);
$('leaveButton').onclick = async () => {
  send({ type: 'leave' });
  state.ws?.close();
  state.peers.forEach(({ pc }) => pc.close());
  state.peers.clear();
  state.localStream?.getTracks().forEach((track) => track.stop());
  state.localStream = null;
  state.room = null;
  $('videos').innerHTML = '';
  $('roomPanel').classList.add('hidden');
  $('addRandomButton').disabled = true;
  await updateDirectory();
};
$('chatForm').onsubmit = (event) => {
  event.preventDefault();
  const text = $('chatInput').value.trim();
  if (text) send({ type: 'chat', text });
  $('chatInput').value = '';
};
$('directoryOptIn').onchange = updateDirectory;
window.addEventListener('beforeunload', () => {
  if (state.session) navigator.sendBeacon('/api/directory', new Blob([JSON.stringify({ session_id: state.session.id, display_name: state.session.display_name, room_id: null, available: false })], { type: 'application/json' }));
});

if (state.session) setSession(state.session);
refreshDirectory().catch(console.warn);
setInterval(refreshDirectory, 5000);
