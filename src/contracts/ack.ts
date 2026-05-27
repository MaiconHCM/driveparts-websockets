export type AckResponse<T> = {
  ok: true;
  data: T;
} | {
  ok: false;
  error: {
    code: string;
    message: string;
  };
  data?: Record<string, unknown>;
};

export function ok_ack<T>(data: T): AckResponse<T> {
  return { ok: true, data };
}

export function error_ack(code: string, message: string, data?: Record<string, unknown>): AckResponse<never> {
  const response: AckResponse<never> = {
    ok: false,
    error: { code, message }
  };

  if (data && Object.keys(data).length > 0) {
    return {
      ...response,
      data
    };
  }

  return response;
}
