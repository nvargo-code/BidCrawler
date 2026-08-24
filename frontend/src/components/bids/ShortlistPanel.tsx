"use client";
import { useState } from "react";
import { X, Send, Trash2, Bookmark, Loader2, Check } from "lucide-react";
import type { Bid } from "@/types/bid";
import { useSavedRecipients } from "@/hooks/useSavedRecipients";

interface ShortlistPanelProps {
  bids: Bid[];
  onRemove: (id: string) => void;
  onClear: () => void;
}

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

export function ShortlistPanel({ bids, onRemove, onClear }: ShortlistPanelProps) {
  const { recipients: savedRecipients, loading: loadingSaved, refresh: refreshSaved } = useSavedRecipients();
  const [showCompose, setShowCompose] = useState(false);
  const [selected, setSelected] = useState<string[]>([]);
  const [newEmail, setNewEmail] = useState("");
  const [sending, setSending] = useState(false);
  const [sendError, setSendError] = useState<string | null>(null);
  const [sent, setSent] = useState(false);

  const addRecipient = (raw: string) => {
    const email = raw.trim();
    if (!email || !EMAIL_RE.test(email)) return;
    setSelected(prev => (prev.includes(email) ? prev : [...prev, email]));
    setNewEmail("");
  };

  const removeRecipient = (email: string) => {
    setSelected(prev => prev.filter(e => e !== email));
  };

  const handleSend = async () => {
    if (selected.length === 0 || sending) return;
    setSending(true);
    setSendError(null);
    try {
      const res = await fetch("/api/send-shortlist", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ bids, recipients: selected }),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        throw new Error(data.error || "Failed to send email");
      }
      setSent(true);
      refreshSaved();
      setTimeout(() => {
        setSent(false);
        setShowCompose(false);
        setSelected([]);
      }, 1800);
    } catch (e) {
      setSendError(e instanceof Error ? e.message : "Failed to send email");
    } finally {
      setSending(false);
    }
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

  const suggestions = savedRecipients.filter(e => !selected.includes(e));

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
          <p className="text-sm font-medium text-foreground">Send comparison matrix to:</p>

          {/* Selected recipient chips */}
          {selected.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {selected.map(email => (
                <span
                  key={email}
                  className="inline-flex items-center gap-1 bg-accent/10 text-accent text-xs rounded-full px-2.5 py-1"
                >
                  {email}
                  <button onClick={() => removeRecipient(email)} title="Remove">
                    <X size={12} />
                  </button>
                </span>
              ))}
            </div>
          )}

          <input
            type="email"
            value={newEmail}
            onChange={e => setNewEmail(e.target.value)}
            onKeyDown={e => {
              if (e.key === "Enter") {
                e.preventDefault();
                addRecipient(newEmail);
              }
            }}
            placeholder="name@example.com — press Enter to add"
            autoFocus
            className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm text-foreground placeholder:text-muted focus:outline-none focus:border-accent"
          />

          {/* Saved recipient suggestions */}
          {!loadingSaved && suggestions.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {suggestions.map(email => (
                <button
                  key={email}
                  onClick={() => addRecipient(email)}
                  className="text-xs rounded-full border border-border px-2.5 py-1 text-muted hover:text-foreground hover:border-accent transition-colors"
                >
                  + {email}
                </button>
              ))}
            </div>
          )}

          {sendError && <p className="text-xs text-red-400">{sendError}</p>}

          <div className="flex gap-2">
            <button
              onClick={() => {
                setShowCompose(false);
                setSendError(null);
              }}
              className="flex-1 py-2 rounded-lg border border-border text-sm text-muted hover:text-foreground transition-colors"
            >
              Cancel
            </button>
            <button
              onClick={handleSend}
              disabled={selected.length === 0 || sending}
              className="flex-1 py-2 rounded-lg bg-accent text-white text-sm font-medium flex items-center justify-center gap-2 disabled:opacity-40 transition-opacity"
            >
              {sent ? (
                <>
                  <Check size={14} /> Sent!
                </>
              ) : sending ? (
                <>
                  <Loader2 size={14} className="animate-spin" /> Sending…
                </>
              ) : (
                <>
                  <Send size={14} /> Send Email
                </>
              )}
            </button>
          </div>
          <p className="text-[11px] text-muted text-center">
            Sends a comparison table for all {bids.length} bids to {selected.length || "…"} recipient
            {selected.length === 1 ? "" : "s"}
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
