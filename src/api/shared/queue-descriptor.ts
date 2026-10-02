const delimiter = ";";

export function encodeQueueDescriptor(tokenId: string, endpoint: string) {
  return [tokenId, endpoint].map(encodeURIComponent).join(delimiter);
}

export function decodeQueueDescriptor(id: string) {
  const [tokenId, endpoint] = id.split(delimiter).map(decodeURIComponent);

  return {
    tokenId,
    endpoint,
  };
}

/**
 * Azure対応でクライアントは endpoint を1回エンコードした状態で送るため、
 * 保存されている endpoint もその形になっている。API の外へ返すときは平文の URL に戻す。
 * 平文のまま保存されたもの（"://" を含む）はそのまま返す。
 */
export function isPlainEndpoint(endpoint: string) {
  return endpoint.includes("://");
}

export function toPlainEndpoint(endpoint: string) {
  if (isPlainEndpoint(endpoint)) {
    return endpoint;
  }

  try {
    return decodeURIComponent(endpoint);
  } catch {
    return endpoint;
  }
}
