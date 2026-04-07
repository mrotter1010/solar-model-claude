const COOKIE_NAME = 'user_id';
const MAX_AGE_DAYS = 365;

/** Read user ID from cookie. Returns string or null. */
export function getUserId() {
  const match = document.cookie.match(
    new RegExp(`(?:^|; )${COOKIE_NAME}=([^;]*)`)
  );
  return match ? decodeURIComponent(match[1]) : null;
}

/** Get existing user ID or create a new one (1-year expiry, SameSite=Strict). */
export function getOrCreateUserId() {
  let id = getUserId();
  if (!id) {
    id = crypto.randomUUID();
    const maxAge = MAX_AGE_DAYS * 24 * 60 * 60;
    document.cookie = `${COOKIE_NAME}=${encodeURIComponent(id)}; max-age=${maxAge}; path=/; SameSite=Strict`;
  }
  return id;
}

/** Build a headers object with X-User-Id if one is stored. */
export function userIdHeaders() {
  const id = getUserId();
  return id ? { 'X-User-Id': id } : {};
}
