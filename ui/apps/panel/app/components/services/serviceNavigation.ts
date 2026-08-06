type PreviewOrigin = {
  spec: {
    baseServiceId: string;
  };
};

function pullRequestsServiceId(currentServiceId: string, preview?: PreviewOrigin): string {
  return preview?.spec.baseServiceId ?? currentServiceId;
}

export { pullRequestsServiceId };
