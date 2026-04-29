"use server";

import { revalidatePath } from "next/cache";
import { getSupabaseServer } from "@/lib/supabase";

async function setStatus(id: string, status: "approved" | "rejected" | null) {
  const sb = getSupabaseServer();
  const { error } = await sb
    .from("operator_enrichment_drafts")
    .update({
      validation_status: status,
      validated_at: status ? new Date().toISOString() : null,
      validated_by: status ? "web-ui" : null,
    })
    .eq("id", id);
  if (error) throw new Error(error.message);
  revalidatePath("/drafts");
}

export async function approveDraft(formData: FormData) {
  const id = String(formData.get("id") ?? "");
  if (id) await setStatus(id, "approved");
}

export async function rejectDraft(formData: FormData) {
  const id = String(formData.get("id") ?? "");
  if (id) await setStatus(id, "rejected");
}

export async function resetDraft(formData: FormData) {
  const id = String(formData.get("id") ?? "");
  if (id) await setStatus(id, null);
}
