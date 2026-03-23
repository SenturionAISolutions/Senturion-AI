-- Client-facing portal: `clinic` role (upload claims + view audit reports only; no internal treasury).
-- App maps profiles.role = 'clinic' via _normalize_profile_role().
--
-- If you use a CHECK constraint on profiles.role, extend it to include 'clinic', e.g.:
--   ALTER TABLE public.profiles DROP CONSTRAINT IF EXISTS profiles_role_check;
--   ALTER TABLE public.profiles ADD CONSTRAINT profiles_role_check
--     CHECK (role IN ('pending_review', 'admin', 'agent', 'client', 'clinic'));
--
COMMENT ON COLUMN public.profiles.role IS
  'RBAC: admin | agent | client | clinic | pending_review. clinic = restricted clinic portal in Streamlit app.';
