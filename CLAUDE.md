# Instructions

- When the user shares coding preferences, patterns, or review feedback that should apply going forward, automatically add them as guidelines to the relevant section of this CLAUDE.md file.
- Don't uncomment any code unless I explicitly ask you to. Most of the time, the code is commented to get rid of it soon after testing.
- Do not change bahvior of the code unless you are explicitly asked to do so, even if you think there's a but or whatever.

# Architecture

- Dont update package.json or Cargo.toml manually. Use pnpm to install/update npm packages and cargo install to install/update rust packages.

# Coding style

- Dont write unnecessary comments. Keep the code self-explanatory. Dont write comments to mark sections of the code either. You should also not remove the comments that are written by the user.
- Only abstract functions that are used in multiple places OR a single function becomes too big/complex. Keep the function names concise and descriptive.
- Use descriptive variable names. Never use variable names that are one or two characters long.
- Variables should be declared right before/near the first usage.
- Helper functions should always be added after the main/exported functions - at the bottom of the file.
- Instead of exporting individual functions, group all exports at the bottom of the file.
- Try not to pollute the global or file level scope; Make the scope as narrow as possible without sacrificing readability.
- Don't export functions/variables that are not used/needed outside the file.
- Use already installed packages and components whenever possible; dont create new ones unless really necessary.
- If you are asked to write mock implementations, keep the mock implementation separate from the core logic and real implementation into its own separate file.
- Dont duplicate Typescript types if they are related to the same entity. Reuse the existing types as much as possible and use Partial, Pick, Omit, etc. to create derived types if needed.
- Dont use early return if it's possible to use if/else statements instead.
- Also, prefer to use `if(condition) {...}` over `if(!condition) return; ...`. Only use early return if it's absolutely necessary.
- In Rust specifically, avoid guard-clause early returns in helper functions unless they are absolutely required for correctness.

# Typescript coding style

- If you are defining several types related to a single entity, group them together inside a namespace and export the namespace instead of the types individually. Don't create deeply nested namespaces though.
- Prefer type over interface.
- Import type statically instead of doing `typeof import("./store").SomeType`
- Dont use typecheck conditional like `typeof node.finishedAt === "number"` too much. This is almost never necessary.
- Use `[].map(...)`, `[].filter(...)`, `[].find(...)`, etc instead of for loop
- Dont use realy return, instead use if/else

# React/SolidJS coding style

- Inline prop type if there's less than 4 props.
- Set the type of the props, not the type of the component.
- If there's more than one contional in JSX, use <Switch...>/<Match...> instead of <Show>...</Show>.
- The component that's exported from the file should come before internal components.
- In SolidJS, use `createStore` instead of multiple `createSignal` calls when managing several related pieces of state.
- In SolidJS, use `produce` from `solid-js/store` for store updates that mutate multiple fields at once. For single-field updates, use `setState("key", value)` directly.
- Avoid large inline render functions inside `<Show>` callbacks. Extract a child component instead.
- Please dont over optimize the code - dont use createMemo in solid-js for everything; use it only for values that are derived from several other states.

# CSS styling

- Always use Tailwind CSS classes.
- For conditional styling, use clsx. Instead of `clsx(collapsed ? "..." :"...")`, you should always set conditional classes like this: `clsx("...", { "...": collapsed, "...": !collapsed })`.
- You should never use variables inside className attribute using template literals. Always use clsx or cn to set conditional classes.
