import {
  QueryObserver,
  useQueryClient,
  type useQuery as useTanstackQuery
} from "@tanstack/solid-query";
import { createComputed, createSignal, onCleanup } from "solid-js";

// Solid-query backs `data` with a resource, so a refetch can suspend the nearest <Suspense> and
// remount the page. Observing the query directly keeps every refresh a plain signal update.
const useQuery = ((options: Parameters<typeof useTanstackQuery>[0]) => {
  const client = useQueryClient();
  const observer = new QueryObserver(client, client.defaultQueryOptions(options()));
  const [result, setResult] = createSignal(
    observer.getOptimisticResult(client.defaultQueryOptions(options())),
    { equals: false }
  );

  createComputed(() => {
    const defaulted = client.defaultQueryOptions(options());
    observer.setOptions(defaulted);
    setResult(observer.getOptimisticResult(defaulted));
  });
  onCleanup(observer.subscribe(setResult));

  return new Proxy({} as ReturnType<typeof useTanstackQuery>, {
    get: (_, property) => Reflect.get(result(), property)
  });
}) as typeof useTanstackQuery;

export { useQuery };
