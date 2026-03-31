import { X } from 'lucide-react';

export default function ErrorBanner({ message, onDismiss }) {
  return (
    <div className="mx-4 mt-4 flex items-center justify-between rounded-lg border border-red-300 bg-red-50 px-4 py-2 text-sm text-red-800">
      <span>{message}</span>
      <button
        onClick={onDismiss}
        className="ml-4 rounded p-1 hover:bg-red-100"
      >
        <X size={14} />
      </button>
    </div>
  );
}
