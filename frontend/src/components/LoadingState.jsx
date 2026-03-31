import { Loader2 } from 'lucide-react';

export default function LoadingState({ message = 'Running analysis...' }) {
  return (
    <div className="flex items-center gap-2 text-gray-500 text-sm pl-2">
      <Loader2 size={16} className="animate-spin" />
      <span>{message}</span>
    </div>
  );
}
