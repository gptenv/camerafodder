# Camera Fodder

Camera Fodder is a first-version Rust + modern JavaScript random video chat application inspired by the classic anonymous pairing flow, updated for small WebRTC rooms.

## Features in this version

- Guest sessions with no account required.
- Local account signup/sign-in with Argon2 password hashes and a JSON-backed auth/session store.
- OAuth provider buttons are represented in the UI as placeholders for future Google, Facebook, and additional provider integrations.
- Random matching into rooms of up to 8 participants, defaulting to 2.
- Dedicated `/room/:room_id` links first show a request-to-join page where visitors can start a guest/local session, choose a display name, and ask to enter before joining the video room.
- Accepted users enter a video-first conference layout with chat, attendee list, call controls, and join-request tools.
- Any participant can call in another random person unless the room was started with host-controlled add-person permissions.
- Host-control rooms are intentionally excluded from matching with other host-control rooms.
- Optional room share links.
- Global Directory opt-in works for signed-in and anonymous users, shows room membership, and lets people request to join listed rooms.
- Room participants get a non-intrusive join-request panel where they can review and accept pending directory requests.
- WebSocket signaling for browser-to-browser WebRTC audio/video and room chat.

## Run locally

```bash
cargo run
```

Open <http://localhost:3000> in two or more browser windows. Camera/microphone access requires a browser context that allows `getUserMedia` (localhost is allowed by modern browsers).

Auth data is stored at `data/auth.json` by default. Override it with:

```bash
AUTH_STORE_PATH=/path/to/auth.json cargo run
```

## Deploy to Cloud Run

Prerequisites: enable Cloud Run, Cloud Build, and Artifact Registry APIs, then authenticate the `gcloud` CLI.

```bash
export PROJECT_ID=your-gcp-project
export REGION=us-central1
./scripts/deploy-cloud-run.sh
```

You can also deploy through Cloud Build:

```bash
gcloud artifacts repositories create camerafodder --repository-format=docker --location=us-central1
gcloud builds submit --config cloudbuild.yaml --substitutions _REGION=us-central1,_SERVICE=camerafodder
```

The container honors Cloud Run's `PORT` environment variable. `AUTH_STORE_PATH` defaults to `/tmp/camerafodder/auth.json` in the image, which is fine for smoke testing but ephemeral on Cloud Run. For durable production auth, mount persistent storage or replace the JSON store with a managed database such as Cloud SQL or Firestore.

## Development notes

Rooms, directory presence, and join requests are runtime state. Restarting the server clears live rooms and directory entries, but account/session data is restored from `AUTH_STORE_PATH`. Production hardening should add OAuth callbacks, moderation/reporting, TURN servers, rate limiting, and stronger room access controls.
