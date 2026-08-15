const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
const publicDir = __dirname;
const EVENTS_FILE = path.join(__dirname, 'auction-events.json');
const LEGACY_STATE_FILE = path.join(__dirname, 'auction-state.json');
const BIDDER_PASSWORD = 'Fifa6';
const MAX_HISTORY = 300;

app.use(express.static(publicDir));
app.use(express.json({ limit: '2mb' }));

const playerNames = ['Prasanta da', 'Sushil da', 'Debo da', 'Sagnik Maz', 'Ajay da', 'Ayan', 'Indra da', 'Nepal da', 'Niloy', 'Koustab', 'Santu da', 'Prem', 'Nil da', 'Susanta da', 'Rana da', 'Extra', 'Sagnik Dutta', 'Agradip', 'Subhrodip', 'Soham'];
function createState() {
  return {
    teams: ['A', 'B', 'C', 'D', 'E'].map((letter) => ({ name: `Team ${letter}`, budget: 1000, players: [] })),
    playerNames: [...playerNames], wheelPlayers: [...playerNames], currentPlayer: null,
    currentBid: { amount: 0, teamIndex: null, teamName: '' }, soldPlayers: [], unsoldPlayers: [],
    countdownSeconds: 0, spinRevealSeconds: 0, auctionStatus: 'Spin the wheel to reveal the next player.',
    wheelRotation: 0, spinning: false, spinId: 0, spinTargetIndex: null, spinWheelPlayers: [], history: []
  };
}
function makeEvent(name) {
  const now = Date.now();
  return { id: crypto.randomUUID(), name: name.trim() || `Auction ${new Date(now).toLocaleDateString()}`, status: 'live', locked: false, createdAt: now, updatedAt: now, completedAt: null, state: createState() };
}
function loadEvents() {
  try {
    const saved = JSON.parse(fs.readFileSync(EVENTS_FILE, 'utf8'));
    if (Array.isArray(saved.events)) return saved.events.map(normalizeEvent);
  } catch (_) { /* first start or corrupt file */ }
  try {
    if (fs.existsSync(LEGACY_STATE_FILE)) {
      const event = makeEvent('BPL Auction (migrated)');
      event.state = normalizeEvent({ state: JSON.parse(fs.readFileSync(LEGACY_STATE_FILE, 'utf8')) }).state;
      log(event, 'Previous single-auction data migrated into this event.');
      return [event];
    }
  } catch (_) { /* use a new event if legacy data is invalid */ }
  return [makeEvent('BPL Auction')];
}
function normalizeEvent(event) {
  const fresh = makeEvent(event.name || 'BPL Auction');
  const state = { ...createState(), ...(event.state || event), history: Array.isArray((event.state || event).history) ? (event.state || event).history : [] };
  state.countdownSeconds = 0; state.spinRevealSeconds = 0; state.spinning = false; state.spinTargetIndex = null; state.spinWheelPlayers = [];
  if (state.currentPlayer) state.auctionStatus = `Server restarted. Resume bidding on ${state.currentPlayer}.`;
  return { ...fresh, ...event, id: event.id || fresh.id, locked: event.locked === true, status: event.status === 'completed' ? 'completed' : 'live', state };
}
let events = loadEvents();
const countdownTimers = new Map();
saveEvents();
function saveEvents() {
  const safe = events.map((event) => ({ ...event, state: { ...event.state } }));
  fs.writeFileSync(EVENTS_FILE, JSON.stringify({ events: safe }, null, 2), 'utf8');
}
function summary(event) {
  return { id: event.id, name: event.name, status: event.status, locked: event.locked === true, createdAt: event.createdAt, updatedAt: event.updatedAt, completedAt: event.completedAt, soldCount: event.state.soldPlayers.length, remainingCount: event.state.wheelPlayers.length + event.state.unsoldPlayers.length + (event.state.currentPlayer ? 1 : 0) };
}
function getEvent(id) { return events.find((event) => event.id === id); }
function isBidder(message) { return message.role === 'Bidder' && message.password === BIDDER_PASSWORD; }
// ---- Video/audio presence + WebRTC signaling relay ----
// Each connected client that has joined an event carries: peerId, name, role,
// and its self-reported + bidder-forced mic/camera state. The server never
// touches media itself - it only relays signaling messages between browsers
// and tells a client to turn its own track off when a Bidder mutes/hides it.
function peerInfo(ws) {
  return { peerId: ws.peerId, name: ws.name, role: ws.role, audioEnabled: ws.audioEnabled !== false && !ws.forcedMuted, videoEnabled: ws.videoEnabled !== false && !ws.forcedVideoOff, forcedMuted: !!ws.forcedMuted, forcedVideoOff: !!ws.forcedVideoOff };
}
function peersInEvent(eventId, excludeWs) {
  const result = [];
  for (const client of wss.clients) if (client !== excludeWs && client.eventId === eventId && client.peerId) result.push(peerInfo(client));
  return result;
}
function broadcastToEvent(eventId, payload, excludeWs) {
  const data = JSON.stringify(payload);
  for (const client of wss.clients) if (client.readyState === 1 && client.eventId === eventId && client !== excludeWs) client.send(data);
}
function findPeer(eventId, peerId) {
  for (const client of wss.clients) if (client.eventId === eventId && client.peerId === peerId) return client;
  return null;
}
// Moderation state (forced mute/hide) keyed by the client's own stable peerId,
// so a network blip + reconnect doesn't silently clear a bidder's mute/hide.
const peerModeration = new Map(); // peerId -> { forcedMuted, forcedVideoOff }
function log(event, message) {
  event.state.history.unshift({ id: crypto.randomUUID(), time: Date.now(), message });
  event.state.history.length = Math.min(event.state.history.length, MAX_HISTORY);
}
function saveAndBroadcast(event) {
  event.updatedAt = Date.now(); saveEvents();
  const payload = JSON.stringify({ type: 'state', event: summary(event), state: event.state });
  for (const client of wss.clients) if (client.readyState === 1 && client.eventId === event.id) client.send(payload);
}
function completeIfFinished(event) {
  const s = event.state;
  if (!s.currentPlayer && s.wheelPlayers.length === 0 && s.unsoldPlayers.length === 0) {
    event.status = 'completed'; event.completedAt = Date.now(); s.auctionStatus = 'Auction completed. All players have been sold.'; log(event, 'Auction completed.');
  }
}
function finalizeCurrentPlayer(event) {
  const s = event.state;
  if (s.currentBid.teamIndex === null) { s.unsoldPlayers.push(s.currentPlayer); log(event, `${s.currentPlayer} went unsold.`); }
  else { const team = s.teams[s.currentBid.teamIndex]; team.budget -= s.currentBid.amount; team.players.push(s.currentPlayer); s.soldPlayers.unshift({ player: s.currentPlayer, team: team.name, amount: s.currentBid.amount }); log(event, `${s.currentPlayer} SOLD to ${team.name} for ${s.currentBid.amount} pts.`); }
  s.currentPlayer = null; s.currentBid = { amount: 0, teamIndex: null, teamName: '' }; s.countdownSeconds = 0;
  if (!s.wheelPlayers.length && s.unsoldPlayers.length) { s.wheelPlayers = s.unsoldPlayers.splice(0); s.auctionStatus = 'Unsold players are back on the wheel for another round.'; }
  else { s.auctionStatus = 'Player processed. Spin the wheel for the next player.'; completeIfFinished(event); }
}
function beginCountdown(event) {
  const timer = setInterval(() => {
    const s = event.state;
    if (s.countdownSeconds <= 1) { clearInterval(timer); countdownTimers.delete(event.id); finalizeCurrentPlayer(event); return saveAndBroadcast(event); }
    s.countdownSeconds -= 1; s.auctionStatus = `Countdown: ${s.countdownSeconds} seconds left.`; saveAndBroadcast(event);
  }, 1000);
  countdownTimers.set(event.id, timer);
}
function resetEvent(event) { if (countdownTimers.has(event.id)) clearInterval(countdownTimers.get(event.id)); countdownTimers.delete(event.id); event.state = createState(); event.status = 'live'; event.completedAt = null; event.state.auctionStatus = 'Auction reset. Spin the wheel to start again.'; log(event, 'Auction reset by bidder.'); }

app.get('/api/events', (_req, res) => res.json(events.map(summary).sort((a, b) => b.updatedAt - a.updatedAt)));
app.post('/api/events', (req, res) => { const event = makeEvent(String(req.body?.name || '')); log(event, 'Auction event created.'); events.push(event); saveEvents(); res.status(201).json(summary(event)); });
app.post('/api/backups/export', (req, res) => {
  if (req.body?.password !== BIDDER_PASSWORD) return res.status(403).json({ message: 'Bidder password required.' });
  res.json({ format: 'bpl-auction-history-backup-v1', exportedAt: new Date().toISOString(), events });
});
app.post('/api/backups/import', (req, res) => {
  if (req.body?.password !== BIDDER_PASSWORD) return res.status(403).json({ message: 'Bidder password required.' });
  const backup = req.body?.backup;
  if (!backup || typeof backup !== 'object') return res.status(400).json({ message: 'Invalid backup file.' });
  for (const timer of countdownTimers.values()) clearInterval(timer);
  countdownTimers.clear();
  if (backup.format === 'bpl-auction-history-backup-v1' && Array.isArray(backup.events)) {
    events = backup.events.map(normalizeEvent);
  } else {
    // Compatibility with the original live-auction export: either the raw
    // state object or its bpl-auction-backup-v2 wrapper becomes one event.
    const oldState = backup.format === 'bpl-auction-backup-v2' && backup.state ? backup.state : backup;
    if (!Array.isArray(oldState.teams) || !Array.isArray(oldState.playerNames)) {
      return res.status(400).json({ message: 'This is not a recognised BPL auction backup.' });
    }
    const imported = makeEvent(`Imported auction ${new Date().toLocaleDateString()}`);
    imported.state = normalizeEvent({ state: oldState }).state;
    imported.state.auctionStatus = imported.state.currentPlayer
      ? `Imported backup. Resume bidding on ${imported.state.currentPlayer}.`
      : 'Imported legacy auction backup.';
    completeIfFinished(imported);
    events = [imported, ...events];
  }
  saveEvents();
  for (const client of wss.clients) if (client.readyState === 1) client.close(1012, 'Auction history restored');
  res.json({ restored: events.length });
});
app.patch('/api/events/:id/lock', (req, res) => {
  const event = getEvent(req.params.id);
  if (!event) return res.status(404).json({ message: 'Auction event not found.' });
  if (req.body?.password !== BIDDER_PASSWORD) return res.status(403).json({ message: 'Bidder password required.' });
  event.locked = req.body?.locked === true; log(event, event.locked ? 'Auction event locked by bidder.' : 'Auction event unlocked by bidder.'); saveAndBroadcast(event);
  res.json(summary(event));
});
app.delete('/api/events/:id', (req, res) => {
  const event = getEvent(req.params.id);
  if (!event) return res.status(404).json({ message: 'Auction event not found.' });
  if (req.body?.password !== BIDDER_PASSWORD) return res.status(403).json({ message: 'Bidder password required.' });
  if (countdownTimers.has(event.id)) clearInterval(countdownTimers.get(event.id)); countdownTimers.delete(event.id);
  events = events.filter((item) => item.id !== event.id); saveEvents();
  for (const client of wss.clients) if (client.readyState === 1 && client.eventId === event.id) client.close(1008, 'Auction event deleted');
  res.status(204).end();
});

wss.on('connection', (ws) => {
  ws.on('close', () => { if (ws.eventId && ws.peerId) broadcastToEvent(ws.eventId, { type: 'peer-left', peerId: ws.peerId }, ws); });
  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch (_) { return; }
    if (msg.type === 'join') {
      const event = getEvent(msg.eventId);
      if (!event) return ws.send(JSON.stringify({ type: 'error', message: 'Auction event not found.' }));
      if (event.locked && !isBidder(msg)) return ws.send(JSON.stringify({ type: 'error', message: 'This auction is locked. Only the bidder can join.' }));
      const alreadyInRoom = ws.eventId === event.id && ws.peerId; // true on the 5s heartbeat re-join, not on first join
      ws.eventId = event.id;
      ws.role = String(msg.role || 'Audience');
      ws.name = String(msg.name || '').trim().slice(0, 40) || ws.role;
      if (!ws.peerId) {
        // Reuse the peerId the client already has (sent after its first join) so a
        // dropped/reconnected socket is recognised as the SAME participant instead
        // of minting a fresh peerId every time — that used to blow away forced
        // mute/hide state and force a full mesh teardown+rebuild for everyone.
        const requested = typeof msg.peerId === 'string' && /^[a-zA-Z0-9-]{1,64}$/.test(msg.peerId) ? msg.peerId : null;
        ws.peerId = requested && !findPeer(event.id, requested) ? requested : crypto.randomUUID();
        const saved = peerModeration.get(ws.peerId);
        if (saved) { ws.forcedMuted = saved.forcedMuted; ws.forcedVideoOff = saved.forcedVideoOff; }
      }
      if (ws.audioEnabled === undefined) ws.audioEnabled = true;
      if (ws.videoEnabled === undefined) ws.videoEnabled = true;
      ws.send(JSON.stringify({ type: 'state', event: summary(event), state: event.state, peerId: ws.peerId }));
      if (!alreadyInRoom) {
        ws.send(JSON.stringify({ type: 'existing-peers', peers: peersInEvent(event.id, ws) }));
        broadcastToEvent(event.id, { type: 'peer-joined', peer: peerInfo(ws) }, ws);
      }
      return;
    }
    const event = getEvent(ws.eventId || msg.eventId);
    if (!event) return;
    if (msg.type === 'webrtc-signal') {
      const target = findPeer(ws.eventId, msg.to);
      if (target && target.readyState === 1) target.send(JSON.stringify({ type: 'webrtc-signal', from: ws.peerId, signalType: msg.signalType, data: msg.data }));
      return;
    }
    if (msg.type === 'media-state') {
      ws.audioEnabled = msg.audioEnabled !== false;
      ws.videoEnabled = msg.videoEnabled !== false;
      broadcastToEvent(ws.eventId, { type: 'peer-media-update', peerId: ws.peerId, audioEnabled: ws.audioEnabled && !ws.forcedMuted, videoEnabled: ws.videoEnabled && !ws.forcedVideoOff }, null);
      return;
    }
    if (msg.type === 'moderate') {
      if (!isBidder(msg)) return ws.send(JSON.stringify({ type: 'error', message: 'Only the bidder can manage participants.' }));
      const target = findPeer(ws.eventId, msg.target);
      if (!target) return;
      if (msg.action === 'kick') {
        peerModeration.delete(target.peerId);
        target.send(JSON.stringify({ type: 'kicked' }));
        broadcastToEvent(ws.eventId, { type: 'peer-left', peerId: target.peerId }, target);
        target.close(4001, 'Kicked by bidder');
        return;
      }
      if (msg.action === 'mute') { target.forcedMuted = true; target.send(JSON.stringify({ type: 'force-media', kind: 'audio', enabled: false })); }
      else if (msg.action === 'unmute') { target.forcedMuted = false; target.send(JSON.stringify({ type: 'force-media', kind: 'audio', enabled: true })); }
      else if (msg.action === 'video-off') { target.forcedVideoOff = true; target.send(JSON.stringify({ type: 'force-media', kind: 'video', enabled: false })); }
      else if (msg.action === 'video-on') { target.forcedVideoOff = false; target.send(JSON.stringify({ type: 'force-media', kind: 'video', enabled: true })); }
      else return;
      if (target.forcedMuted || target.forcedVideoOff) peerModeration.set(target.peerId, { forcedMuted: !!target.forcedMuted, forcedVideoOff: !!target.forcedVideoOff });
      else peerModeration.delete(target.peerId);
      broadcastToEvent(ws.eventId, { type: 'peer-media-update', peerId: target.peerId, audioEnabled: target.audioEnabled && !target.forcedMuted, videoEnabled: target.videoEnabled && !target.forcedVideoOff }, null);
      return;
    }
    const s = event.state;
    if (event.status === 'completed' && msg.type !== 'import') { s.auctionStatus = 'This auction is completed and is read-only.'; return saveAndBroadcast(event); }
    if (msg.type === 'reset') { if (!isBidder(msg)) { s.auctionStatus = 'Only the bidder can reset the auction.'; } else resetEvent(event); return saveAndBroadcast(event); }
    if (msg.type === 'import') {
      if (!isBidder(msg) || !msg.state || typeof msg.state !== 'object') { s.auctionStatus = 'Import failed or is not authorized.'; return saveAndBroadcast(event); }
      const incoming = msg.state && msg.state.state && (msg.state.format || msg.state.event) ? msg.state.state : msg.state;
      if (countdownTimers.has(event.id)) clearInterval(countdownTimers.get(event.id)); countdownTimers.delete(event.id);
      event.state = normalizeEvent({ state: incoming }).state; event.status = 'live'; event.completedAt = null;
      event.state.auctionStatus = event.state.currentPlayer ? `Auction imported. Resume bidding on ${event.state.currentPlayer}.` : 'Auction imported successfully.';
      log(event, 'Auction state imported by bidder.'); return saveAndBroadcast(event);
    }
    if (msg.type === 'deleteHistory') { if (isBidder(msg)) s.history = s.history.filter((entry) => entry.id !== msg.id); return saveAndBroadcast(event); }
    if (msg.type === 'setPlayers') {
      if (!isBidder(msg)) { s.auctionStatus = 'Only the bidder can change auction players.'; return saveAndBroadcast(event); }
      if (s.currentPlayer || s.soldPlayers.length || s.unsoldPlayers.length || s.countdownSeconds > 0) { s.auctionStatus = 'Player list can only be changed before the auction starts.'; return saveAndBroadcast(event); }
      if (!Array.isArray(msg.players)) { s.auctionStatus = 'Invalid player list.'; return saveAndBroadcast(event); }
      const players = [...new Set(msg.players.map((name) => String(name).trim()).filter((name) => name.length > 0 && name.length <= 60))].slice(0, 100);
      if (!players.length) { s.auctionStatus = 'Choose at least one player.'; return saveAndBroadcast(event); }
      s.playerNames = players; s.wheelPlayers = [...players]; s.auctionStatus = `${players.length} players saved for this auction.`; log(event, `Player list updated (${players.length} players).`);
      return saveAndBroadcast(event);
    }
    if (msg.type === 'spin') {
      if (!isBidder(msg)) s.auctionStatus = 'Only the bidder can spin the wheel.';
      else if (s.currentPlayer || !s.wheelPlayers.length) s.auctionStatus = 'Finish the current player before spinning again.';
      else { const index = Number.isInteger(+msg.selectedIndex) && +msg.selectedIndex >= 0 && +msg.selectedIndex < s.wheelPlayers.length ? +msg.selectedIndex : Math.floor(Math.random() * s.wheelPlayers.length); s.currentPlayer = s.wheelPlayers.splice(index, 1)[0]; s.currentBid = { amount: 0, teamIndex: null, teamName: '' }; s.auctionStatus = `Selected ${s.currentPlayer}. Place your bid now.`; log(event, `Revealed: ${s.currentPlayer}.`); }
      return saveAndBroadcast(event);
    }
    if (msg.type === 'bid') {
      const index = s.teams.findIndex((team) => team.name === msg.role), amount = Number(msg.amount);
      if (!s.currentPlayer) s.auctionStatus = 'Spin the wheel first.';
      else if (index < 0) s.auctionStatus = 'Only a team captain can place a bid.';
      else if (!Number.isFinite(amount) || amount <= s.currentBid.amount || amount > s.teams[index].budget) s.auctionStatus = 'Enter a higher valid bid within your team budget.';
      else { s.currentBid = { amount, teamIndex: index, teamName: s.teams[index].name }; s.auctionStatus = `${s.teams[index].name} placed ${amount} pts for ${s.currentPlayer}.`; log(event, s.auctionStatus); }
      return saveAndBroadcast(event);
    }
    if (msg.type === 'startCountdown') {
      if (!isBidder(msg) || !s.currentPlayer) { s.auctionStatus = !isBidder(msg) ? 'Only the bidder can sell a player.' : 'Spin the wheel first.'; return saveAndBroadcast(event); }
      if (s.countdownSeconds > 0) { s.auctionStatus = 'Countdown is already running.'; return saveAndBroadcast(event); }
      s.countdownSeconds = 10; s.auctionStatus = 'Countdown: 10 seconds left.'; log(event, `Bid countdown started for ${s.currentPlayer}.`); beginCountdown(event); return saveAndBroadcast(event);
    }
  });
});

app.get('/', (_req, res) => res.sendFile(path.join(publicDir, 'history.html')));
server.listen(process.env.PORT || 3000, () => console.log(`Auction server running on http://localhost:${process.env.PORT || 3000}`));
