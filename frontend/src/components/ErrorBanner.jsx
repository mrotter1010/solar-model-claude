import { X } from 'lucide-react';

export default function ErrorBanner({ message, onDismiss }) {
  return (
    <div className="mx-4 mt-4 flex items-center justify-between rounded-lg border border-red-800/50 bg-red-950/30 px-4 py-2 text-sm text-vantyra-error">
      <span>{message}</span>
      <button
        onClick={onDismiss}
        className="ml-4 rounded p-1 text-vantyra-error hover:text-red-300"
      >
        <X size={14} />
      </button>
    </div>
  );
}
