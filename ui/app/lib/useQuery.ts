import { useQuery as useTanstackQuery } from "@tanstack/solid-query";

const useQuery = ((...args: Parameters<typeof useTanstackQuery>) => {
  const query = useTanstackQuery(...args);
  return new Proxy(query, {
    get(target, property, receiver) {
      if (property === "data" && !target.dataUpdatedAt && !target.isPlaceholderData) {
        return undefined;
      } else {
        return Reflect.get(target, property, receiver);
      }
    }
  });
}) as typeof useTanstackQuery;

export { useQuery };
