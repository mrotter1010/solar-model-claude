import { useState } from 'react';
import { CheckCircle, XCircle, ChevronDown, ChevronRight } from 'lucide-react';

function formatToolName(name) {
  return name
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase());
}

export default function StepsList({ steps }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="mt-3 border-t border-green-200 pt-2">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-1 text-xs text-gray-500 hover:text-gray-700"
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
                  <CheckCircle size={14} className="text-green-600 shrink-0" />
                ) : (
                  <XCircle size={14} className="text-red-600 shrink-0" />
                )}
                <span className="text-gray-700">{formatToolName(step.tool)}</span>
              </div>
              {!step.success && step.error && (
                <p className="ml-6 text-xs text-red-600">{step.error}</p>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
