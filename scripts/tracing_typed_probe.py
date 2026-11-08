"""Typed span lifecycle and actual OTLP transport inspection."""
RESULT_REPORT='tracing-typed-results.json'
from integration import ARTIFACTS
from tracing_transport_probe import main
if __name__=='__main__':main(ARTIFACTS/'tools/v-tracing',RESULT_REPORT,'typed-export')
