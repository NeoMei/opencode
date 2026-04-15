import { spyOn } from "bun:test"
import path from "path"
import { Effect } from "effect"
import { TuiConfig } from "../../src/config/tui"

type PluginSpec = string | [string, Record<string, unknown>]

/**
 * Mock `TuiConfig.Service.use` so callers that do
 * `AppRuntime.runPromise(TuiConfig.Service.use(svc => svc.get()))` receive
 * the provided config object instead of loading from disk.
 *
 * Returns a restore function.
 */
export function mockTuiService(config: TuiConfig.Info, opts?: { wait?: () => Effect.Effect<void> }) {
  const mock: TuiConfig.Interface = {
    get: () => Effect.succeed(config),
    waitForDependencies: () => opts?.wait?.() ?? Effect.void,
  }
  const spy = spyOn(TuiConfig.Service, "use" as never).mockImplementation(((fn: (svc: TuiConfig.Interface) => any) =>
    fn(mock)) as never)
  return () => spy.mockRestore()
}

/**
 * Full mock: sets OPENCODE_PLUGIN_META_FILE, mocks cwd, and mocks
 * TuiConfig.Service with the given plugins.
 */
export function mockTuiRuntime(dir: string, plugin: PluginSpec[]) {
  process.env.OPENCODE_PLUGIN_META_FILE = path.join(dir, "plugin-meta.json")
  const plugin_origins = plugin.map((spec) => ({
    spec,
    scope: "local" as const,
    source: path.join(dir, "tui.json"),
  }))
  const restore = mockTuiService({ plugin, plugin_origins })
  const cwd = spyOn(process, "cwd").mockImplementation(() => dir)

  return () => {
    cwd.mockRestore()
    restore()
    delete process.env.OPENCODE_PLUGIN_META_FILE
  }
}
