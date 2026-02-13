import logging

import finchvox
from dotenv import load_dotenv
from livekit.agents import Agent, AgentServer, AgentSession, JobContext, cli, inference
from livekit.plugins import silero

load_dotenv()

logger = logging.getLogger("finchvox-livekit-example")

tracer_provider = finchvox.init_livekit(
    service_name="livekit-agent",
    endpoint="http://localhost:4317",
    metadata={"agent.version": "1.0.0"},
)


class VoiceAgent(Agent):
    def __init__(self) -> None:
        super().__init__(
            instructions="You are a helpful voice assistant. Keep responses brief and conversational.",
            stt=inference.STT("deepgram/nova-3"),
            llm=inference.LLM("openai/gpt-4o-mini"),
            tts=inference.TTS("cartesia/sonic"),
        )

    async def on_enter(self):
        self.session.generate_reply(instructions="Greet the user warmly.")


server = AgentServer()


@server.rtc_session()
async def entrypoint(ctx: JobContext):
    async def flush_traces():
        tracer_provider.force_flush()

    ctx.add_shutdown_callback(flush_traces)

    session = AgentSession(vad=silero.VAD.load())
    await session.start(agent=VoiceAgent(), room=ctx.room)


if __name__ == "__main__":
    cli.run_app(server)
