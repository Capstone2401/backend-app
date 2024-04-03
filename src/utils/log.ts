function error(error: Error): void {
  console.log("Error: " + error.message);
}

function info(...values: any): void {
  console.log(...values);
}

export default {
  error,
  info,
};
