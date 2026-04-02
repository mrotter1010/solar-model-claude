import { Loader2, CheckCircle, XCircle } from 'lucide-react';

function formatToolName(name) {
  return name
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase());
}

export default function ExecutionProgress({ steps }) {
  return (
    <div className="flex justify-start">
      <div className="max-w-[75%] rounded-lg border border-blue-200 bg-blue-50 px-4 py-3">
        <p className="text-xs font-semibold uppercase tracking-wide text-blue-700 mb-2">
          Executing Plan
        </p>
        <div className="space-y-2">
          {steps.map((step) => (
            <div key={step.step_number} className="flex items-center gap-2 text-sm">
              {step.status === 'running' && (
                <Loader2 size={14} className="animate-spin text-blue-600 shrink-0" />
              )}
              {step.status === 'complete' && step.success && (
                <CheckCircle size={14} className="text-green-600 shrink-0" />
              )}
              {step.status === 'complete' && !step.success && (
                <XCircle size={14} className="text-red-600 shrink-0" />
              )}
              <span className="text-gray-700">{formatToolName(step.tool_name)}</span>
              {step.status === 'complete' && !step.success && (
                <span className="text-xs text-red-500 ml-1">failed</span>
              )}
            </div>
          ))}
          {steps.length > 0 && steps.every(s => s.status === 'complete') && (
            <div className="flex items-center gap-2 text-sm pt-1 border-t border-blue-100 mt-1">
              <Loader2 size={14} className="animate-spin text-blue-600 shrink-0" />
              <span className="text-blue-700">Generating summary…</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
