import { observations } from './providers.ts';

export default function generateQuery(query: string, _options?: unknown) {
  observations.queries.push(query);
  return Promise.resolve({ semantic_query: query, lexical_query: query });
}
