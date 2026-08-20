"use client";
import { useState } from "react";
import { X, Send, Trash2, Bookmark } from "lucide-react";
import type { Bid } from "@/types/bid";

interface ShortlistPanelProps {
  bids: Bid[];
  onRemove: (id: string) => void;
  onClear: () => void;
}

function buildEmailBody(bids: Bid[]): string {
  return bids
    .map((bid, i) => {
      const due = bid.due_date ?? "No due date";
      const county = bid.location_county ? ` · ${bid.location_county} County` : "";
      return `${i + 1}. ${bid.title ?? "Untitled"}
   Agency: ${bid.agency ?? "Unknown"}${county}
   Due: ${due}
   ${bid.bid_url ?? "No URL"}`;
    })
    .join("\n\n");
}

export function ShortlistPanel({ bids, onRemove, onClear }: ShortlistPanelProps) {
  const [emailTo, setEmailTo] = useState("");
  const [showCompose, setShowCompose] = useState(false);

  const handleSend = () => {
    const subject = encodeURIComponent(
      `Bid Shortlist — ${bids.length} project${bids.length === 1 ? "" : "s"}`
    );
    const body = encodeURIComponent(buildEmailBody(bids));
    const to = encodeURIComponent(emailTo.trim());
    window.open(`mailto:${to}?subject=${subject}&body=${body}`);
  };

  if (bids.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-20 text-center gap-3">
        <Bookmark size={36} className="text-muted opacity-40" />
        <p className="text-foreground font-medium">No saved bids yet</p>
        <p className="text-sm text-muted">
          Swipe right on a bid (mobile) or tap the bookmark icon to save it here
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {/* Header row */}
      <div className="flex items-center justify-between py-1">
        <span className="text-sm text-muted">
          {bids.length} saved bid{bids.length !== 1 ? "s" : ""}
        </span>
        <button
          onClick={onClear}
          className="text-xs text-muted hover:text-red-400 flex items-center gap-1 transition-colors"
        >
          <Trash2 size={12} /> Clear all
        </button>
      </div>

      {/* Bid list */}
      {bids.map(bid => (
        <div key={bid.id} className="rounded-xl bg-card border border-border p-4 flex items-start gap-3">
          <div className="flex-1 min-w-0">
            <p className="text-sm font-semibold text-foreground line-clamp-2 leading-snug">
              {bid.title ?? "Untitled"}
            </p>
            <p className="text-xs text-muted mt-1">
              {bid.agency ?? "Unknown agency"}
              {bid.location_county ? ` · ${bid.location_county} Co.` : ""}
            </p>
            {bid.due_date && (
              <p className="text-xs text-muted mt-0.5">Due {bid.due_date}</p>
            )}
            {bid.bid_url && (
              <a
                href={bid.bid_url}
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-accent hover:underline mt-1 inline-block"
              >
                Open bid →
              </a>
            )}
          </div>
          <button
            onClick={() => onRemove(bid.id)}
            className="text-muted hover:text-red-400 transition-colors mt-0.5 flex-shrink-0"
            title="Remove"
          >
            <X size={16} />
          </button>
        </div>
      ))}

      {/* Email compose */}
      {showCompose ? (
        <div className="rounded-xl bg-card border border-accent/40 p-4 space-y-3 mt-4">
          <p className="text-sm font-medium text-foreground">Send to email address:</p>
          <input
            type="email"
            value={emailTo}
            onChange={e => setEmailTo(e.target.value)}
            onKeyDown={e => e.key === "Enter" && emailTo && handleSend()}
            placeholder="name@example.com"
            autoFocus
            className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm text-foreground placeholder:text-muted focus:outline-none focus:border-accent"
          />
          <div className="flex gap-2">
            <button
              onClick={() => setShowCompose(false)}
              className="flex-1 py-2 rounded-lg border border-border text-sm text-muted hover:text-foreground transition-colors"
            >
              Cancel
            </button>
            <button
              onClick={handleSend}
              disabled={!emailTo.trim()}
              className="flex-1 py-2 rounded-lg bg-accent text-white text-sm font-medium flex items-center justify-center gap-2 disabled:opacity-40 transition-opacity"
            >
              <Send size={14} /> Open in Email
            </button>
          </div>
          <p className="text-[11px] text-muted text-center">
            Opens your email app with all {bids.length} bids pre-filled
          </p>
        </div>
      ) : (
        <button
          onClick={() => setShowCompose(true)}
          className="w-full py-3 rounded-xl bg-accent text-white font-medium flex items-center justify-center gap-2 hover:bg-accent/90 transition-colors mt-2"
        >
          <Send size={16} /> Send as Email
        </button>
      )}
    </div>
  );
}
