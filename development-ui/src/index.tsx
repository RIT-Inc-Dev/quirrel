import type React from "react";
import {
  createContext,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";
import { GlobalSearchProvider } from "./hooks/useGlobalSearch";
import { QuirrelProvider } from "./hooks/useQuirrel";
import { BaseLayout } from "./layouts/BaseLayout";
import Activity from "./pages/activity";
import Cron from "./pages/cron";
import Pending from "./pages/pending";

export type Route = "cron" | "activity-log" | "pending";

export type QuirrelDevelopmentUIProps = {
  router: {
    initial: Route;
    onChange(route: Route): void;
    listenToNavigationChanges(onChange: (newRoute: Route) => void): () => void;
  };
  config: {
    fixedEndpoint?: string;
    authentication: {
      enabled: boolean;
    };
    linkToAPIDocs?: string;
  };
};

const pageMap: Record<Route, React.FunctionComponent> = {
  "activity-log": Activity,
  cron: Cron,
  pending: Pending,
};

type Router = {
  current: Route;
  navigate(route: Route): void;
};

export const RouterContext = createContext<Router>({
  current: "activity-log",
  navigate() {},
});

export const ConfigContext = createContext<QuirrelDevelopmentUIProps["config"]>(
  {
    authentication: {
      enabled: true,
    },
  },
);

export function QuirrelDevelopmentUI(props: QuirrelDevelopmentUIProps) {
  const { initial, onChange, listenToNavigationChanges } = props.router;
  const [route, setRoute] = useState(initial);
  const navigate = useCallback(
    (newRoute: Route) => {
      setRoute(newRoute);
      onChange(newRoute);
    },
    [setRoute, onChange],
  );

  const router = useMemo<Router>(
    () => ({ current: route, navigate }),
    [route, navigate],
  );

  useEffect(() => {
    return listenToNavigationChanges(setRoute);
  }, [listenToNavigationChanges, setRoute]);

  const Page = pageMap[route];

  return (
    <ConfigContext.Provider value={props.config}>
      <RouterContext.Provider value={router}>
        <GlobalSearchProvider>
          <QuirrelProvider>
            <BaseLayout>
              <Page />
            </BaseLayout>
          </QuirrelProvider>
        </GlobalSearchProvider>
      </RouterContext.Provider>
    </ConfigContext.Provider>
  );
}
