import { useQuery as suspendingUseQuery } from "@tanstack/solid-query";
import { untrack } from "solid-js";

// Solid Query registers a pending data read with the nearest Suspense boundary.
// Background refetches can then detach the route DOM for a frame, resetting
// scroll and form state. Track status/dataUpdatedAt while reading data outside
// reactive tracking so feature packages share the same non-suspending behavior.
const useQuery = ((options: never, queryClient?: never) => {
  const query = suspendingUseQuery(options, queryClient);
  return new Proxy(query, {
    get(target, prop) {
      if (prop === "data") {
        if (target.status === "pending") return undefined;
        void target.dataUpdatedAt;
        return untrack(() => Reflect.get(target, "data"));
      }
      return Reflect.get(target, prop);
    }
  });
}) as typeof suspendingUseQuery;

export { useQuery };
