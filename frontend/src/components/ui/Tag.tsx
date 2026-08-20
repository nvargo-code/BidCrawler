"use client";

import { cn } from "@/lib/utils";

interface TagProps {
  children: React.ReactNode;
  selected?: boolean;
  onClick?: () => void;
  size?: "sm" | "md";
  className?: string;
}

export function Tag({ children, selected = false, onClick, size = "md", className }: TagProps) {
  const sizes = { sm: "px-2 py-0.5 text-xs", md: "px-3 py-1.5 text-sm" };
  return (
    <span
      role={onClick ? "button" : undefined}
      tabIndex={onClick ? 0 : undefined}
      onClick={onClick}
      onKeyDown={onClick ? (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onClick(); } } : undefined}
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full font-medium transition-colors",
        sizes[size],
        selected ? "bg-accent text-white" : "bg-card border border-border text-foreground",
        onClick && "cursor-pointer hover:border-accent/50",
        className
      )}
    >
      {children}
    </span>
  );
}
