// supabase/functions/api/index.ts

import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_ANON_KEY = Deno.env.get("SUPABASE_ANON_KEY")!;
const GRIP_API_KEY = Deno.env.get("GRIP_API_KEY")!;
const GRIP_BASE = "https://gripsystem.onrender.com";

// Simple in‑memory rate limiter (per Edge instance)
const RATE_LIMIT = 30; // requests
const WINDOW_MS = 60_000;
const hits = new Map<string, { count: number; since: number }>();

serve(async (req) => {
  const ip = req.headers.get("x-forwarded-for") ?? "unknown";
  const now = Date.now();

  const entry = hits.get(ip) ?? { count: 0, since: now };
  if (now - entry.since < WINDOW_MS) {
    entry.count++;
    if (entry.count > RATE_LIMIT) {
      console.warn("[RATE_LIMIT]", ip);
      return new Response("Rate limit exceeded", { status: 429 });
    }
  } else {
    entry.count = 1;
    entry.since = now;
  }
  hits.set(ip, entry);

  const authHeader = req.headers.get("Authorization");
  if (!authHeader) {
    return new Response("Missing Authorization header", { status: 401 });
  }

  const supabase = createClient(
    SUPABASE_URL,
    SUPABASE_ANON_KEY,
    { global: { headers: { Authorization: authHeader } } }
  );

  const { data: { user }, error } = await supabase.auth.getUser();
  if (error || !user) {
    return new Response("Unauthorized", { status: 401 });
  }

  const role = user.user_metadata?.role ?? "standard";
  const url = new URL(req.url);

  let gripPath: string;
  if (url.pathname.endsWith("/decision")) {
    gripPath = "/decision";
  } else if (url.pathname.endsWith("/override")) {
    if (role !== "governance") {
      return new Response("Forbidden: governance role required", { status: 403 });
    }
    gripPath = "/override";
  } else {
    return new Response("Not found", { status: 404 });
  }

  const body = await req.text();

  console.log(JSON.stringify({
    ts: new Date().toISOString(),
    user_id: user.id,
    role,
    gripPath
  }));

  const gripResp = await fetch(`${GRIP_BASE}${gripPath}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": GRIP_API_KEY
    },
    body
  });

  const respText = await gripResp.text();

  return new Response(respText, {
    status: gripResp.status,
    headers: { "Content-Type": "application/json" }
  });
});