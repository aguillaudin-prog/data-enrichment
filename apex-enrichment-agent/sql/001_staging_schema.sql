-- APEX Enrichment Agent — staging table schema
-- Run this once on the same Supabase project as APEX.
--
-- This table is WRITE-ONLY for the enrichment agent.
-- The APEX UI READS from it for human validation, then promotes
-- validated rows to the production operators table.

create table if not exists operator_enrichment_drafts (
    id uuid primary key default gen_random_uuid(),

    -- identity (composite uniqueness)
    operator_name text not null,
    email text not null,
    source_url text not null,

    -- proof of provenance (anti-hallucination)
    snippet text not null,                  -- 50 chars before + email + 50 after
    fetched_at timestamptz not null,
    method text not null,                   -- 'regex' | 'llm_assist' | 'post_check'

    -- scoring & validation
    score int not null default 0,
    is_best boolean not null default false,
    mx_valid boolean not null default false,
    post_check_failed boolean,              -- null = not yet checked, true/false after J+1

    -- human review workflow (set by APEX UI)
    validated_by text,
    validated_at timestamptz,
    validation_status text,                 -- 'approved' | 'rejected' | 'needs_more_info'
    validation_notes text,

    -- bookkeeping
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    constraint uniq_draft unique (operator_name, email, source_url)
);

create index if not exists idx_drafts_operator on operator_enrichment_drafts (operator_name);
create index if not exists idx_drafts_pending_review on operator_enrichment_drafts (validation_status)
    where validation_status is null;
create index if not exists idx_drafts_post_check on operator_enrichment_drafts (post_check_failed)
    where post_check_failed is null;

-- Auto-update updated_at on every row update
create or replace function set_updated_at() returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

drop trigger if exists trg_drafts_updated_at on operator_enrichment_drafts;
create trigger trg_drafts_updated_at
    before update on operator_enrichment_drafts
    for each row execute function set_updated_at();

-- Optional: RLS policies. Adjust based on your Supabase auth model.
-- alter table operator_enrichment_drafts enable row level security;
