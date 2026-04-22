import { useState } from 'react';
import { CheckCircle, XCircle, ChevronDown, ChevronRight } from 'lucide-react';

function formatToolName(name) {
  return name
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase());
}

/**
 * Format a step error for display. Handles plain strings and
 * JSON-encoded error objects (e.g. batch validation 422 responses).
 */
function formatStepError(error) {
  if (typeof error !== 'string') return String(error);

  // Try to parse JSON error strings (e.g. from result_summary)
  try {
    const parsed = JSON.parse(error);
    // Batch validation error with structured errors array
    if (parsed.errors && Array.isArray(parsed.errors)) {
      return parsed.errors
        .map(e => `Row ${e.row}, ${e.column}: ${e.message}`)
        .join('\n');
    }
    // Simple {error: "...", tool: "..."} from executor
    if (parsed.error && typeof parsed.error === 'string') {
      return parsed.error;
    }
  } catch {
    // Not JSON — return as-is
  }
  return error;
}

export default function StepsList({ steps }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="mt-3 border-t border-vantyra-border pt-2">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-1 text-xs text-vantyra-text-s hover:text-vantyra-text"
      >
        {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        {expanded ? 'Hide execution details' : 'Show execution details'}
      </button>
      {expanded && (
        <div className="mt-2 space-y-1">
          {steps.map((step, idx) => (
            <div key={idx} className="text-sm">
              <div className="flex items-center gap-2">
                {step.success ? (
                  <CheckCircle size={14} className="text-vantyra-success shrink-0" />
                ) : (
                  <XCircle size={14} className="text-vantyra-error shrink-0" />
                )}
                <span className="text-vantyra-text-s">{formatToolName(step.tool)}</span>
              </div>
              {!step.success && step.error && (
                <p className="ml-6 text-xs text-vantyra-error whitespace-pre-wrap">
                  {formatStepError(step.error)}
                </p>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
