"use client";

import { forwardRef, HTMLAttributes } from "react";
import { cn } from "@/lib/utils";

export interface CardProps extends HTMLAttributes<HTMLDivElement> {
  variant?: "default" | "interactive";
  padding?: "none" | "sm" | "md" | "lg";
}

export const Card = forwardRef<HTMLDivElement, CardProps>(
  ({ className, variant = "default", padding = "md", children, ...props }, ref) => {
    const paddings = { none: "", sm: "p-3", md: "p-4", lg: "p-6" };
    return (
      <div
        ref={ref}
        className={cn(
          "rounded-xl bg-card border border-border",
          variant === "interactive" && "cursor-pointer hover:bg-card-hover transition-colors",
          paddings[padding],
          className
        )}
        {...props}
      >
        {children}
      </div>
    );
  }
);
Card.displayName = "Card";
