import { useQuery as suspendingUseQuery } from "@tanstack/solid-query";
import { untrack } from "solid-js";

// Reading `query.data` through solid-query's own getter registers the read with
// the nearest <Suspense> boundary (the router wraps every route in one) whenever
// the backing resource hasn't settled — e.g. on first read, or when an observer
// mounts against an already-populated cache. That registration re-triggers the
// boundary on every background refetch, detaching the page DOM for a frame and
// resetting every scroll position and <select> selection under it. Tracking
// status/dataUpdatedAt and reading data untracked keeps the reactivity while
// never touching the suspending path.
const useQuery = ((options: never, queryClient?: never) => {
  const query = suspendingUseQuery(options, queryClient);
  return new Proxy(query, {
    get(target, prop) {
      if (prop === "data") {
        if (target.status === "pending") {
          return undefined;
        } else {
          void target.dataUpdatedAt;
          return untrack(() => Reflect.get(target, "data"));
        }
      } else {
        return Reflect.get(target, prop);
      }
    }
  });
}) as typeof suspendingUseQuery;

export { useQuery };
