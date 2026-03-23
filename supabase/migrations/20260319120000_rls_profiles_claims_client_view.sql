-- =============================================================================
-- Senturion BPO Sniper — RLS hardening & client isolation
-- Run via Supabase CLI (`supabase db push`) or SQL Editor.
--
-- Prerequisites:
--   • public.profiles: primary key `id` uuid REFERENCES auth.users(id)
--   • public.claims: column `user_id` uuid NOT REFERENCES auth.users(id) (row owner)
--   • Adjust column names in policies/view if your schema differs.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1) Row Level Security: profiles
-- ---------------------------------------------------------------------------
ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.profiles FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "profiles_select_own" ON public.profiles;
CREATE POLICY "profiles_select_own"
  ON public.profiles
  FOR SELECT
  TO authenticated
  USING (id = auth.uid());

DROP POLICY IF EXISTS "profiles_update_own" ON public.profiles;
CREATE POLICY "profiles_update_own"
  ON public.profiles
  FOR UPDATE
  TO authenticated
  USING (id = auth.uid())
  WITH CHECK (id = auth.uid());

-- ---------------------------------------------------------------------------
-- 2) Row Level Security: claims (client owns row; staff may access all)
-- ---------------------------------------------------------------------------
ALTER TABLE public.claims ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.claims FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "claims_select_isolation" ON public.claims;
CREATE POLICY "claims_select_isolation"
  ON public.claims
  FOR SELECT
  TO authenticated
  USING (
    user_id = auth.uid()
    OR EXISTS (
      SELECT 1
      FROM public.profiles p
      WHERE p.id = auth.uid()
        AND lower(coalesce(p.role, '')) IN ('admin', 'agent')
    )
  );

DROP POLICY IF EXISTS "claims_insert_own" ON public.claims;
CREATE POLICY "claims_insert_own"
  ON public.claims
  FOR INSERT
  TO authenticated
  WITH CHECK (
    user_id = auth.uid()
    OR EXISTS (
      SELECT 1
      FROM public.profiles p
      WHERE p.id = auth.uid()
        AND lower(coalesce(p.role, '')) IN ('admin', 'agent')
    )
  );

DROP POLICY IF EXISTS "claims_update_isolation" ON public.claims;
CREATE POLICY "claims_update_isolation"
  ON public.claims
  FOR UPDATE
  TO authenticated
  USING (
    user_id = auth.uid()
    OR EXISTS (
      SELECT 1
      FROM public.profiles p
      WHERE p.id = auth.uid()
        AND lower(coalesce(p.role, '')) IN ('admin', 'agent')
    )
  )
  WITH CHECK (
    user_id = auth.uid()
    OR EXISTS (
      SELECT 1
      FROM public.profiles p
      WHERE p.id = auth.uid()
        AND lower(coalesce(p.role, '')) IN ('admin', 'agent')
    )
  );

-- ---------------------------------------------------------------------------
-- 3) client_view — STRICT client-only projection (always auth.uid())
--    Application code for portal users should SELECT only from this view.
--    security_invoker: policies on underlying `claims` still apply; the view
--    predicate guarantees a client NEVER sees another user's UUID.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS public.client_view;

CREATE VIEW public.client_view
  WITH (security_invoker = true)
AS
SELECT c.*
FROM public.claims c
WHERE c.user_id = auth.uid();

GRANT SELECT ON public.client_view TO authenticated;

COMMENT ON VIEW public.client_view IS
  'Client portal: exposes only claims where user_id = auth.uid(). No cross-tenant rows.';

-- ---------------------------------------------------------------------------
-- 4) Storage (manual in Dashboard)
--    Create bucket: senturion-vault (private). Link signed URLs or download
--    via service role only as appropriate; app uses authenticated uploads.
-- ---------------------------------------------------------------------------
