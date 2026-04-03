import { Loader2 } from 'lucide-react';

export default function LoadingState({ message = 'Running analysis...' }) {
  return (
    <div className="flex items-center gap-2 text-sm pl-2">
      <Loader2 size={16} className="animate-spin text-vantyra-accent" />
      <span className="text-vantyra-text-s">{message}</span>
    </div>
  );
}
