import { useQuery } from "@tanstack/react-query";
import { api } from "./api";
import type { ScreenId } from "./types";

/** A screen's payload (always the recorded fixture, so it's cached for the session). */
export function useScreen<S extends ScreenId>(screen: S) {
  return useQuery({ queryKey: ["screen", screen], queryFn: () => api.screen(screen), staleTime: Infinity, retry: 1 });
}
