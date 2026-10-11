// Provider boundaries only. Production handlers, auth, cache, and logInsert stay real.
export const observations = {
  redisExists: [] as string[],
  redisWrites: [] as Array<{ email: string; value: unknown; options: unknown }>,
  logDocuments: [] as Array<Record<string, unknown>>,
  logIndexes: [] as string[],
  logins: [] as Array<{ email: string; password: string }>,
  metadata: [] as string[],
  queries: [] as string[],
  bigqueryJobs: 0,
};

export const remoteEmails = new Set<string>();
export const deniedEmails = new Set<string>();

export function resetObservations() {
  observations.redisExists.length = 0;
  observations.redisWrites.length = 0;
  observations.logDocuments.length = 0;
  observations.logIndexes.length = 0;
  observations.logins.length = 0;
  observations.metadata.length = 0;
  observations.queries.length = 0;
  observations.bigqueryJobs = 0;
}

export class Redis {
  constructor(_options: unknown) {}

  exists(email: string) {
    observations.redisExists.push(email);
    return Promise.resolve(remoteEmails.has(email) ? 1 : 0);
  }

  set(email: string, value: unknown, options: unknown) {
    observations.redisWrites.push({ email, value, options });
    if (remoteEmails.has(email)) return Promise.resolve(null);
    remoteEmails.add(email);
    return Promise.resolve('OK');
  }

  setex(email: string, seconds: number, value: unknown) {
    observations.redisWrites.push({ email, value, options: { ex: seconds } });
    remoteEmails.add(email);
    return Promise.resolve('OK');
  }
}

export const defaultProvider = () => () => Promise.resolve({});
export const AwsSigv4Signer = (_options: unknown) => ({});

export class Client {
  constructor(_options: unknown) {}

  index(request: { index: string; body: Record<string, unknown> }) {
    observations.logIndexes.push(request.index);
    observations.logDocuments.push(request.body);
    return Promise.resolve({});
  }

  search(_request: unknown) {
    return Promise.resolve({ body: { hits: { hits: [] } } });
  }

  mget(_request: unknown) {
    return Promise.resolve({ body: { docs: [] } });
  }
}

export class Pinecone {
  constructor(_options: unknown) {}

  index(_name: string) {
    return {
      namespace: (_namespace: string) => ({
        query: (_options: unknown) => Promise.resolve({ matches: [] }),
      }),
    };
  }
}

export class SupabaseClient {
  auth = {
    signInWithPassword: (credentials: { email: string; password: string }) => {
      observations.logins.push({ ...credentials });
      return Promise.resolve({
        data: {
          user: { role: 'authenticated' },
          session: { access_token: 'offline-session' },
        },
        error: deniedEmails.has(credentials.email) ? { message: 'Invalid credentials' } : null,
      });
    },
  };

  rpc(name: string, _args?: unknown) {
    observations.metadata.push(name);
    return Promise.resolve({ data: [], error: null });
  }

  from(name: string) {
    observations.metadata.push(name);
    return {
      select: (_fields: string) => {
        const result = { data: [], error: null };
        return {
          ...result,
          in: (_field: string, _values: string[]) => Promise.resolve(result),
        };
      },
    };
  }
}

export const createClient = (_url: string, _key: string, _options?: unknown) =>
  new SupabaseClient();

export class BigQuery {
  constructor(_options: unknown) {}

  createQueryJob(_options: unknown) {
    observations.bigqueryJobs++;
    return Promise.resolve([
      {
        id: 'offline-job',
        getQueryResults: () => Promise.resolve([[]]),
      },
    ]);
  }
}
