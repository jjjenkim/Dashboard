-- Team Korea Dashboard Supabase schema
-- Run this once in Supabase SQL Editor.

create extension if not exists pgcrypto;

create table if not exists public.athletes (
  fis_code text primary key,
  id text,
  name_ko text,
  name_en text,
  birth_date date,
  birth_year int,
  age int,
  sport text,
  sport_display text,
  team text,
  fis_url text,
  current_rank int,
  best_rank int,
  season_starts int,
  medals jsonb not null default '{"gold":0,"silver":0,"bronze":0}'::jsonb,
  source_updated_at timestamptz,
  synced_at timestamptz not null default now(),
  sync_run_id text not null
);

create table if not exists public.athlete_results (
  result_uid text primary key,
  fis_code text not null references public.athletes(fis_code) on delete cascade,
  event_date date,
  place text,
  category text,
  discipline text,
  rank int,
  rank_status text,
  fis_points numeric,
  cup_points numeric,
  source_updated_at timestamptz,
  synced_at timestamptz not null default now(),
  sync_run_id text not null
);

create index if not exists idx_athlete_results_fis_code on public.athlete_results (fis_code);
create index if not exists idx_athlete_results_date on public.athlete_results (event_date desc);

create table if not exists public.sync_logs (
  id uuid primary key default gen_random_uuid(),
  sync_run_id text not null,
  source text not null default 'v7_pipeline',
  success boolean not null,
  athletes_count int,
  results_count int,
  max_event_date date,
  detail jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_sync_logs_run_id on public.sync_logs(sync_run_id);
create index if not exists idx_sync_logs_created_at on public.sync_logs(created_at desc);

alter table public.athletes enable row level security;
alter table public.athlete_results enable row level security;
alter table public.sync_logs enable row level security;

-- Public read policies for dashboard rendering
do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public' and tablename = 'athletes' and policyname = 'public read athletes'
  ) then
    create policy "public read athletes" on public.athletes
    for select to anon using (true);
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public' and tablename = 'athlete_results' and policyname = 'public read athlete_results'
  ) then
    create policy "public read athlete_results" on public.athlete_results
    for select to anon using (true);
  end if;
end
$$;

-- sync_logs is for backend/ops only; no anon select policy
