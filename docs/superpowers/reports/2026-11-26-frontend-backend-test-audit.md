# RCaster Frontend/Backend and HCI Test Audit

Date: 2026-11-26

What this repo does: RCaster is a Rust NTRIP caster with standalone and
clustered modes. It accepts RTCM/NMEA/BINEX/CMR streams, handles source and
client admission, publishes STR data, relays sources across nodes, serves NEAR,
and provides administrator and public browser interfaces.

Assumed load:

- 1–10 administrators on desktop browsers.
- Up to thousands of public viewers and more than 1,000 visible stations.
- NTRIP source/client counts at the existing capacity targets, including
  10,000 sessions and mountpoints.
- Desktop interaction is primary. Phone/tablet use is possible for the public
  map and directory, so responsive and touch behavior still matters.

Current automated evidence is strong for protocol framing, authentication,
source admission, source fencing, cluster leases, relay, NEAR selection,
configuration watches, backup/recovery and the main admin/public API contracts.
Browser coverage is broad on happy paths, but weak for failure, accessibility,
mobile, pagination, repeated input and long-running UI state.

Implementation progress:

- Findings 1–4 are fixed by commit `a424b6f`.
- Browser evidence:
  `tools/web-e2e/runs/20261126-012446/result.json`.
- Verified in standalone and clustered modes:
  - admin source table loads 150/150 across cursor pages;
  - public directory loads 250/250 across cursor pages;
  - double-click token creation creates exactly one token;
  - all 17 global map clusters are keyboard-focusable.
- Findings 5, 6 and 8 are fixed by commit `49b624e`; finding 7 is fixed for
  the user form and remains for the mountpoint form.
- Browser evidence:
  `tools/web-e2e/runs/20261126-013722/result.json`.
- Finding 7 is now complete, and findings 9–10 are fixed by commit `91b2272`.
- Browser evidence:
  `tools/web-e2e/runs/20261126-014406/result.json`.
- Finding 14 is fixed by commit `98eab6b`.
- Browser evidence:
  `tools/web-e2e/runs/20261126-015017/result.json`; admin and public pages
  have no horizontal overflow at 390×844.
- Finding 12 is partially fixed and finding 16 is partially fixed by commit
  `ce9e785`: STR/source-expectation JSON now validates before submit, all batch
  action types and operation Cancel/Retry controls are present; config diff and
  upgrade mutation controls remain.
- Browser evidence:
  `tools/web-e2e/runs/20261126-015339/result.json`.
- Findings 3, 10-18 are now closed for the local browser
  scope:
  - configuration changes require a field-level diff preview before Apply;
  - destructive actions use an accessible impact dialog with pending disable;
  - paired coordinates, owner, password, radius and JSON errors are identified
    before a write;
  - admin and public views retain data and show `Data stale` with bounded retry;
  - public idle session checks detect expiry and monitor reconnect status is
    tied to the active socket;
  - the browser gate runs Chromium plus Firefox 153.7 ESR in standalone and
    clustered modes.
- Admin and public JavaScript now live in `admin_app.js` and `public_app.js`,
  are served as same-origin `text/javascript` resources, and are covered by
  direct asset endpoint tests instead of being embedded in Rust/HTML strings.
- Final local evidence:
  `tools/web-e2e/runs/20261126-025403/result.json`.
- Firefox found and verified a startup auth race where an old anonymous 401
  could clear a newly authenticated admin session. `api()` now binds 401
  handling to the auth generation captured when the request started.

## Coverage map

| Feature area | Existing proof | Missing proof |
|---|---|---|
| NTRIP1/2 parsing and framing | Core unit/integration tests, real fixtures, real clients | No new gap in the agreed scope |
| Source admission, STR generation and updates | End-to-end upload → RTCM parse → ACTIVE STR tests | Real CMR capture remains external and excluded from client-compatibility acceptance |
| Client/source admission and quotas | Concurrency and final-slot race tests | High-volume browser behavior when thousands of rows are present |
| Cluster source ownership and leases | Fencing, stale-owner, failover and recovery tests | Real four-node rolling upgrade/rollback drill |
| Relay and NEAR | Local relay tests, CRTK snapshot, haversine/index checks | Continuous production-network behavior |
| Admin auth, CSRF and API tokens | API tests for scopes, expiry, revoke and auth-version invalidation | Login failure UX, lockout countdown, token copy/one-time warning UX |
| User/mountpoint/blacklist CRUD | API and browser happy-path CRUD | Concurrent edits, stale ETag UX, duplicate submit, field-level validation |
| Source/session kick | Real socket disconnect tests in standalone and clustered modes | Double-click, disconnect failure and pending-timeout UX |
| Public directory and map | Browser tests, real OSM/AMap tiles, 1,301 CRTK stations, 0 px marker error | More than 200-row directory navigation, keyboard/touch map operation |
| Public monitor | Ticket and WebSocket happy path; generation-change closure | Reconnect, network loss, session expiry and close-reason UX |
| Batch operations | API tests for all actions, retry, cancel and recovery | Browser controls for delete/disable/change-owner/cancel/retry |
| Upgrade workflow | API tests for preview/approve/cancel/rollback | Browser workflow and real multi-node rollout |
| Mobile/accessibility | None beyond viewport metadata and a few ARIA labels | Keyboard, screen reader, focus order, responsive tables, touch targets |

## Findings

### Must fix

1. **[Fixed] The public directory silently stops at 200 stations** (`crates/rcaster-service/src/public.rs:1384`)
   - **What this is:** The public page requests `/api/v1/mountpoints?limit=200` once and never reads `next_cursor`.
   - **Problem:** The CRTK snapshot has 1,301 valid stations, so the map can show all of them while the table lists at most 200. Users cannot find or monitor most stations in the list.
   - **Fix:** Loop `next_cursor` until exhausted, or add a clear “Load more” control. Add a browser test with 201 and 1,301 stations.
   - **If we skip it:** Most of the public directory is invisible even though the backend returns the data.

2. **[Fixed] The admin source table silently stops at 100 sources** (`crates/rcaster-service/src/admin.rs:4145`, `crates/rcaster-service/src/admin.rs:3922`)
   - **What this is:** The API returns `next_cursor`, but `loadSources()` ignores it and uses the default limit of 100.
   - **Problem:** With 101 or more active sources, older mountpoints cannot be seen, inspected or disconnected from the admin page. Filtering does not fix the missing page because the cursor is still ignored.
   - **Fix:** Add cursor pagination or “Load more” to the source table. Test 101 sources and filtered result sets across page boundaries.
   - **If we skip it:** Operators get an incomplete active-source view under normal capacity.

3. **[Fixed] Repeated clicks can create hidden credentials or duplicate writes** (`crates/rcaster-service/src/admin.rs:4169`, `crates/rcaster-service/src/admin.rs:4175`, `crates/rcaster-service/src/admin.rs:4183`)
   - **What this is:** Save/Create/Start buttons remain enabled while requests are running.
   - **Problem:** Double-clicking “Create token” can create two valid tokens. The second secret replaces the first in `api-token-secret`, while the first token remains active but its secret is lost. User/mountpoint saves can similarly produce duplicate requests and confusing 409 errors.
   - **Fix:** Add one in-flight flag per action and disable the initiating button until completion. Clear the token secret before creating another token. Test one double-click produces one token.
   - **If we skip it:** Operators can create unrecorded active credentials or see a failed status after a successful write.

4. **[Fixed] Map stations and clusters cannot be operated by keyboard** (`crates/rcaster-service/src/public.rs:1400`, `crates/rcaster-service/src/public.rs:1408`)
   - **What this is:** SVG circles use `onclick` but have no `tabindex`, button role or keyboard handler.
   - **Problem:** Keyboard and switch users cannot focus a station, open its monitor, or expand a cluster. Screen readers only see a generic image, not individual station controls.
   - **Fix:** Make each marker/cluster `role="button"`, `tabindex="0"`, add an `aria-label`, handle Enter/Space, and add visible focus plus zoom buttons. Test Tab, Enter and Space.
   - **If we skip it:** The main public station-selection flow is mouse-only.

### Should fix

5. **[Fixed] Dynamic status and errors are not announced** (`crates/rcaster-service/src/admin.rs:4107`, `crates/rcaster-service/src/public.rs:1375`)
   - **What this is:** Save, login, disconnect and failure messages are written into `<p id="status">`.
   - **Problem:** The element has no `role="status"` or `aria-live`. A screen-reader user gets no confirmation after a save or disconnect, including failures.
   - **Fix:** Use `role="status" aria-live="polite"` for progress/success and `role="alert"` for errors. Add browser accessibility assertions.
   - **If we skip it:** Non-visual users cannot tell whether an action worked.

6. **[Fixed] Admin auto-refresh destroys table focus and overwrites user feedback** (`crates/rcaster-service/src/admin.rs:4144`, `crates/rcaster-service/src/admin.rs:4186`)
   - **What this is:** Every 10 seconds `load()` replaces table children and sets status to “Loading…” then “Connected”.
   - **Problem:** If focus is on an Edit/Delete button, the node is destroyed and focus drops. A “User saved”, “Disconnect accepted” or error message is also replaced within 10 seconds.
   - **Fix:** Keep the connection indicator separate from action toasts, and update rows by stable key instead of replacing the whole table. Test focus survives refresh and a success message remains visible.
   - **If we skip it:** Keyboard users lose their place and operators miss action outcomes.

7. **[Fixed] Create and edit modes are not visible or cancellable** (`crates/rcaster-service/src/admin.rs:4168`, `crates/rcaster-service/src/admin.rs:4174`)
   - **What this is:** Clicking Edit fills the same form used for Create. There is no mode label, Cancel, Clear or reset after save.
   - **Problem:** After saving an edit, the fields remain and the revision resets to Create mode. Pressing Save again attempts to create a duplicate. Users can also unknowingly overwrite another edit context.
   - **Fix:** Add an explicit “Creating / Editing X” label, Cancel button, and clear/reset after save. Change the button text to “Create” or “Update”.
   - **If we skip it:** Routine CRUD produces duplicate requests and user confusion.

8. **[Fixed] Raw JSON errors are shown as backend text** (`crates/rcaster-service/src/admin.rs:4140`, `crates/rcaster-service/src/public.rs:1382`)
   - **What this is:** `api()` throws the complete response text.
   - **Problem:** A 400/403/409/429 produces raw JSON or an empty body in the status line. Users do not get the offending field, a conflict explanation or a lockout countdown.
   - **Fix:** Parse `{code,message}` once, map common statuses to concise messages, and show field errors beside the form. Test 400, 401, 403, 409, 429 and 503.
   - **If we skip it:** Operators cannot distinguish a bad field from a stale revision or permission failure.

9. **[Fixed] Public/admin live monitors do not recover after WebSocket loss** (`crates/rcaster-service/src/public.rs:1404`, `crates/rcaster-service/src/admin.rs:4166`)
   - **What this is:** `onclose` changes the status text but does not offer retry or restart the socket.
   - **Problem:** A network interruption, ticket expiry or source generation change ends monitoring. The public chart timer also keeps polling every five seconds after the socket closes.
   - **Fix:** Clear timers on close, show the close reason, add a Retry button that requests a new ticket, and test disconnect → reconnect.
   - **If we skip it:** Users may see stale charts and believe monitoring is still active.

10. **[Fixed] Public login state can remain visually “Signed in” after expiry** (`crates/rcaster-service/src/public.rs:1385`, `crates/rcaster-service/src/public.rs:1404`)
    - **What this is:** The page checks login only when the action starts; it does not refresh `/api/v1/auth/session`.
    - **Problem:** After the session expires, the page still says “Signed in” until Monitor is clicked. The eventual failure is raw backend text.
    - **Fix:** Poll the session at a low rate or handle 401 globally by switching to anonymous mode and showing “Session expired”.
    - **If we skip it:** Users repeatedly click actions that cannot succeed.

11. **[Fixed] Forms have no field-level validation or impact preview** (`crates/rcaster-service/src/admin.rs:4112`, `crates/rcaster-service/src/admin.rs:4113`, `crates/rcaster-service/src/admin.rs:4175`)
    - **What this is:** Coordinates, coverage, JSON, owner IDs and passwords are sent first and validated by the server.
    - **Problem:** Blank owner, longitude without latitude, invalid JSON or radius zero only produce a generic error after a round trip.
    - **Fix:** Add required/paired-field checks, JSON parse feedback and a summary of affected sessions/sources before disabling/deleting. Keep the server validation.
    - **If we skip it:** Users discover basic errors late and cannot see the impact before a destructive action.

12. **[Fixed for the local browser scope] Raw JSON fields are too risky for routine administration** (`crates/rcaster-service/src/admin.rs:4113`, `crates/rcaster-service/src/admin.rs:4121`)
    - **What this is:** STR fields, source expectations and the full caster config are edited as JSON.
    - **Problem:** One missing quote or comma loses the edit context and yields a raw parse error. There is no schema example, format button or diff before Apply.
    - **Fix:** Add JSON validation on blur, collapsible field help/examples, and a read-only diff preview before applying config.
    - **If we skip it:** Configuration mistakes become likely during incident response.

13. **[Fixed] Destructive controls have weak context and no busy state** (`crates/rcaster-service/src/admin.rs:4163`, `crates/rcaster-service/src/admin.rs:4170`, `crates/rcaster-service/src/admin.rs:4176`, `crates/rcaster-service/src/admin.rs:4184`)
    - **What this is:** Source/session/user/mountpoint/token actions use `confirm()` and then send the request.
    - **Problem:** The dialog does not show affected users, sessions or active source state, and can be triggered twice before the row refreshes.
    - **Fix:** Use one shared accessible confirmation dialog with target and impact count; disable the action while pending. Keep typed confirmation only for cluster-wide operations.
    - **If we skip it:** A fast double-click or wrong row can disconnect the wrong service.

14. **[Fixed] No mobile or overflow design** (`crates/rcaster-service/src/admin.rs:4105`, `crates/rcaster-service/src/public.rs:1373`)
    - **What this is:** There is a viewport meta tag, but no media query or horizontal table wrapper.
    - **Problem:** On a 390 px phone, wide tables and 360 px inputs grow the page wider than the viewport. Map touch zoom is not explicitly tested, and buttons have small spacing.
    - **Fix:** Add a table scroll container, stack form fields under a mobile breakpoint, and test 390×844 plus 768×1024 with touch.
    - **If we skip it:** Public station lookup and admin actions are hard to use on phones and tablets.

15. **[Fixed for browser data refresh] Offline and slow-network behavior is untested and has no stale-data state** (`crates/rcaster-service/src/admin.rs:4144`, `crates/rcaster-service/src/public.rs:1384`)
    - **What this is:** Both pages refresh on timers and replace data when requests succeed.
    - **Problem:** A failed refresh leaves old data without a timestamp or “stale” marker. Admin switches back to the login screen for most failures; public leaves the last map/table visible.
    - **Fix:** Track last-success time, show “Data stale” without hiding the current view, add bounded retry/backoff, and test offline → online recovery.
    - **If we skip it:** Operators may mistake old state for current state.

16. **[Fixed for the isolated browser scope] Batch operation and upgrade UI cover only part of the backend** (`crates/rcaster-service/src/admin.rs:4123`)
    - **What it is:** The batch selector exposes only `users.disable` and `sessions.drop`; operation lookup has no Cancel/Retry; upgrade UI is lookup-only.
    - **Problem:** Backend actions for user delete, mountpoint disable, owner change, cancel, retry, approve and rollback cannot be exercised through the browser.
    - **Fix:** Add controls in the same existing sections and browser tests for each action’s preview, permission and failure states.
   - **Current state:** All five batch action types, operation Cancel/Retry,
     upgrade Preview/Start/Approve/Cancel/Rollback and read-only control hiding
     are present. The clustered browser run now uses a signed manifest and a
     disposable isolated Caster to verify Preview, pending approval, Cancel,
     Rollback preview digest propagation, Rollback Cancel, and second-admin
     Approve to `RUNNING`; the approved command then shuts down that test
     Caster by design.
   - **Remaining proof:** A real multi-node signed rollout and rollback under
     production traffic remains external acceptance.

17. **[Fixed for Chromium plus Firefox smoke] Current browser gate is a single happy-path Chromium test** (`tools/web-e2e/run_browser_e2e.py:300`)
    - **What it is:** The harness covers broad CRUD and socket behavior, but forces `confirm=true`, uses one headless Chromium, and does not test malformed input or failures.
    - **Problem:** Keyboard, focus, screen reader, touch, offline, duplicate click, stale ETag, login lockout and WebSocket reconnect regressions can all ship green.
    - **Fix:** Split into small scenario tests and add a second browser engine plus mobile viewport, accessibility tree checks and network-failure cases.
    - **If we skip it:** The current “pass” says the happy path works, not that human interaction is safe.

18. **[Fixed] Embedded inline JavaScript is hard to test in isolation** (`crates/rcaster-service/src/admin.rs:4107`, `crates/rcaster-service/src/public.rs:1375`)
    - **What it is:** Admin and public UI code lives in large Rust string literals.
    - **Problem:** Helpers such as state, validation, pagination and reconnect logic cannot be unit tested without booting a full Caster and browser.
    - **Fix:** Move static JavaScript into served asset files with small test exports; keep the same HTML and endpoints.
   - **Current state:** JavaScript is stored in `admin_app.js` and
     `public_app.js`, loaded with `defer` from `/admin/app.js` and `/app.js`.
     Asset endpoints return `text/javascript`, `no-store`, and `nosniff`;
     API tests verify script contents and browser runs verify execution.

## Recommended test order

1. Pagination: public 201/1,301 stations; admin 101 sources.
2. Safety: token double-click; stale ETag; duplicate delete; 429 lockout.
3. Keyboard/accessibility: map marker Enter/Space, status live regions, focus survives auto-refresh.
4. Session and socket recovery: expiry, logout, WebSocket close, reconnect, stale-data indicator.
5. Forms: invalid JSON, paired coordinates, radius/owner errors, create/edit mode.
6. Operations: all batch actions, cancel/retry, upgrade approve/rollback UI.
7. Layout: 390 px phone, 768 px tablet, touch map drag/zoom.
8. Cross-browser: Chromium plus Firefox or WebKit.

Verdict: The backend is comparatively well tested. The next work should focus
on list completeness, duplicate-action safety and browser accessibility before
adding more protocol features.

Not checked: soak servers, real four-node upgrade/DNS/OCSP environments,
screen-reader software, non-Chromium browsers, and high-volume visual
performance on the production deployment.
