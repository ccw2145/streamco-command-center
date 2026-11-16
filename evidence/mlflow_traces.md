# MLflow traces of the Ask agent

Experiment `/Users/<user>/streamco-command-center` (`881368839201646`): 8 traces.

| trace | time | state | ms | spans | tools | question |
|---|---|---|---|---|---|---|
| tr-58229bfd94d03785fe1844bec9d237db | 2026-11-09 20:27 UTC | OK | 34883 | 9 | ask_genie, cancel_drivers, search_knowledge | Break down the Sports cancellation spike by cause. |
| tr-db9d027595515cb9daa5582a7db578b3 | 2026-11-09 20:27 UTC | OK | 34591 | 9 | ask_genie, cancel_drivers, search_knowledge | Why did so many Sports subscribers cancel after the Big Game? |
| tr-4c9e936cc52766018c997dc9162cc00c | 2026-11-09 20:26 UTC | OK | 27383 | 9 | ask_genie, cancel_drivers, search_knowledge | What drove yesterday's Sports cancellations? |
| tr-648235ad1578218d7ae39df3e6f15036 | 2026-11-09 20:26 UTC | OK | 28097 | 9 | ask_genie, cancel_drivers, search_knowledge | Why are Sports cancellations up so much this morning? |
| tr-960803af5e9e25d760becddc475d0102 | 2026-11-09 20:25 UTC | OK | 41017 | 9 | ask_genie, cancel_drivers, search_knowledge | What caused the Sports cancel spike? |
| tr-35802fb3da4efd78561102dc10250050 | 2026-11-09 20:25 UTC | OK | 28085 | 9 | ask_genie, cancel_drivers, search_knowledge | Why did Sports cancellations spike after the Big Game? |
| tr-94391f9d2f592adbd3fa2e834de4adbc | 2026-11-09 20:24 UTC | OK | 42268 | 9 | ask_genie, cancel_drivers, search_knowledge | Why did Sports cancellations jump after the game? |
| tr-3fc7600c80d3004870d011a0e1780277 | 2026-11-09 20:23 UTC | OK | 44515 | 9 | ask_genie, cancel_drivers, search_knowledge | Why did Sports cancellations jump? |

## Spans of tr-58229bfd94d03785fe1844bec9d237db

| span | type | status | ms |
|---|---|---|---|
| streamco-ask | AGENT | SpanStatusCode.OK | 34884 |
| Completions | CHAT_MODEL | SpanStatusCode.OK | 2505 |
| cancel_drivers | TOOL | SpanStatusCode.OK | 830 |
| ask_genie | TOOL | SpanStatusCode.OK | 26269 |
| search_knowledge | TOOL | SpanStatusCode.OK | 825 |
| Embeddings | EMBEDDING | SpanStatusCode.OK | 597 |
| search_knowledge | TOOL | SpanStatusCode.OK | 1300 |
| Embeddings | EMBEDDING | SpanStatusCode.OK | 603 |
| Completions | CHAT_MODEL | SpanStatusCode.OK | 3148 |
