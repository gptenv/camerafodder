# Camera Fodder

Camera Fodder is a first-version Rust + modern JavaScript random video chat application inspired by the classic anonymous pairing flow, updated for small WebRTC rooms.

## Features in this version

- Guest sessions with no account required.
- In-memory local signup/sign-in for development.
- OAuth provider buttons are represented in the UI as placeholders for future Google, Facebook, and additional provider integrations.
- Random matching into rooms of up to 8 participants, defaulting to 2.
- Any participant can call in another random person unless the room was started with host-controlled add-person permissions.
- Host-control rooms are intentionally excluded from matching with other host-control rooms.
- Optional room share links.
- Global Directory opt-in works for signed-in and anonymous users and removes users when they go offline or opt out.
- WebSocket signaling for browser-to-browser WebRTC audio/video and room chat.

## Run locally

```bash
cargo run
```

Open <http://localhost:3000> in two or more browser windows. Camera/microphone access requires a browser context that allows `getUserMedia` (localhost is allowed by modern browsers).

## Development notes

This is intentionally in-memory for the first version. Restarting the server clears accounts, sessions, rooms, and directory entries. Production hardening should add persistent storage, OAuth callbacks, password hashing with per-user salts, moderation/reporting, TURN servers, rate limiting, and stronger room access controls.
