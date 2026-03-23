-- New self-serve signups should land in `pending_review` until an Admin promotes them
-- via User Management → Role directory (admin / agent / client).
--
-- Option A: set column default (adjust if `role` is NOT NULL without default today):
--   ALTER TABLE public.profiles
--     ALTER COLUMN role SET DEFAULT 'pending_review';
--
-- Option B (recommended): handle in `handle_new_user` trigger after insert into auth.users:
--   INSERT INTO public.profiles (id, email, role)
--   VALUES (NEW.id, NEW.email, 'pending_review')
--   ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email;

COMMENT ON TABLE public.profiles IS
  'RBAC: use role pending_review for new accounts; Admins assign admin|agent|client in the app.';
