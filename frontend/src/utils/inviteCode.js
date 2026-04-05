const COOKIE_NAME = 'invite_code';
const MAX_AGE_DAYS = 30;

/** Read invite code from cookie. Returns string or null. */
export function getInviteCode() {
  const match = document.cookie.match(
    new RegExp(`(?:^|; )${COOKIE_NAME}=([^;]*)`)
  );
  return match ? decodeURIComponent(match[1]) : null;
}

/** Store invite code in a cookie (30-day expiry, SameSite=Strict). */
export function setInviteCode(code) {
  const maxAge = MAX_AGE_DAYS * 24 * 60 * 60;
  document.cookie = `${COOKIE_NAME}=${encodeURIComponent(code)}; max-age=${maxAge}; path=/; SameSite=Strict`;
}

/** Remove the invite code cookie. */
export function clearInviteCode() {
  document.cookie = `${COOKIE_NAME}=; max-age=0; path=/; SameSite=Strict`;
}

/** Build a headers object with X-Invite-Code if one is stored. */
export function inviteHeaders() {
  const code = getInviteCode();
  return code ? { 'X-Invite-Code': code } : {};
}
