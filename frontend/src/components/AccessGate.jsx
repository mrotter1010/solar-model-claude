import { useState } from 'react';
import { CONFIG } from '../config.js';
import vantyraIcon from '../assets/vantyra-icon.png';

export default function AccessGate({ onSuccess }) {
  const [code, setCode] = useState('');
  const [error, setError] = useState(null);
  const [isChecking, setIsChecking] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!code.trim()) return;

    setIsChecking(true);
    setError(null);

    try {
      // Validate by hitting a non-exempt endpoint with the invite code.
      // /health is exempt from invite code middleware, so we use
      // /sessions/_validate instead — middleware rejects with 401 if
      // the code is invalid; a 404 means the code passed.
      const res = await fetch(`${CONFIG.orchestratorUrl}/sessions/_validate`, {
        headers: { 'X-Invite-Code': code.trim() },
      });

      if (res.status === 401) {
        setError('Invalid invite code');
      } else {
        onSuccess(code.trim());
      }
    } catch {
      setError('Unable to reach server. Please try again.');
    } finally {
      setIsChecking(false);
    }
  };

  return (
    <div className="h-screen flex items-center justify-center bg-vantyra-bg">
      <div className="w-full max-w-sm rounded-lg border border-vantyra-border bg-vantyra-bg-s p-8">
        <div className="flex flex-col items-center mb-6">
          <img src={vantyraIcon} alt="Vantyra" className="h-16 w-16 object-contain mb-3" />
          <h1 className="text-xl font-semibold text-vantyra-text">
            <span className="text-white">Vantyra</span>{' '}
            <span className="text-vantyra-accent">Analytics</span>
          </h1>
          <p className="mt-1 text-sm text-vantyra-text-s">Enter your invite code to continue</p>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <input
            type="text"
            value={code}
            onChange={(e) => setCode(e.target.value)}
            placeholder="Invite code"
            autoFocus
            className="w-full rounded-md border border-vantyra-border bg-vantyra-bg px-3 py-2 text-vantyra-text placeholder:text-vantyra-text-s focus:border-vantyra-accent focus:outline-none focus:ring-1 focus:ring-vantyra-accent"
          />

          {error && (
            <p className="text-sm text-vantyra-error">{error}</p>
          )}

          <button
            type="submit"
            disabled={isChecking || !code.trim()}
            className="w-full rounded-md bg-vantyra-accent px-4 py-2 text-sm font-medium text-vantyra-bg hover:brightness-110 transition disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isChecking ? 'Checking...' : 'Continue'}
          </button>
        </form>
      </div>
    </div>
  );
}
