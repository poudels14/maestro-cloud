type DefinedProperties<Value> = {
  [Key in keyof Value]?: Exclude<Value[Key], undefined>;
};

function mergeDefinedProperties<Current extends object, Updates extends object>(
  current: Current,
  updates: Updates
): DefinedProperties<Current & Updates> {
  return Object.fromEntries(
    Object.entries({ ...current, ...updates }).filter(([, value]) => value !== undefined)
  ) as DefinedProperties<Current & Updates>;
}

export { mergeDefinedProperties };
export type { DefinedProperties };
