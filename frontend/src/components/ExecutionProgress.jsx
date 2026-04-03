import { Loader2, CheckCircle, XCircle } from 'lucide-react';

function formatToolName(name) {
  return name
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase());
}

export default function ExecutionProgress({ steps }) {
  return (
    <div className="flex justify-start">
      <div className="max-w-[75%] rounded-lg border border-vantyra-border bg-vantyra-bg-s px-4 py-3">
        <p className="text-xs font-semibold uppercase tracking-wide text-vantyra-accent mb-2">
          Executing Plan
        </p>
        <div className="space-y-2">
          {steps.map((step) => (
            <div key={step.step_number} className="flex items-center gap-2 text-sm">
              {step.status === 'running' && (
                <Loader2 size={14} className="animate-spin text-vantyra-accent shrink-0" />
              )}
              {step.status === 'complete' && step.success && (
                <CheckCircle size={14} className="text-vantyra-success shrink-0" />
              )}
              {step.status === 'complete' && !step.success && (
                <XCircle size={14} className="text-vantyra-error shrink-0" />
              )}
              <span className="text-vantyra-text">{formatToolName(step.tool_name)}</span>
              {step.status === 'complete' && !step.success && (
                <span className="text-xs text-vantyra-error ml-1">failed</span>
              )}
            </div>
          ))}
          {steps.length > 0 && steps.every(s => s.status === 'complete') && (
            <div className="flex items-center gap-2 text-sm pt-1 border-t border-vantyra-border mt-1">
              <Loader2 size={14} className="animate-spin text-vantyra-accent shrink-0" />
              <span className="text-vantyra-text-s">Generating summary…</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
