"use client";
import { useState, useEffect, useCallback } from "react";

export function useSavedRecipients() {
  const [recipients, setRecipients] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch("/api/recipients");
      const data = await res.json();
      setRecipients(data.recipients ?? []);
    } catch {
      // Leave existing list as-is on failure.
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  return { recipients, loading, refresh };
}
