import type { NextConfig } from "next";
import path from "path";

const nextConfig: NextConfig = {
  // Tell Next.js this project root is the frontend/ dir, not the monorepo root
  outputFileTracingRoot: path.join(__dirname, "../"),
};

export default nextConfig;
