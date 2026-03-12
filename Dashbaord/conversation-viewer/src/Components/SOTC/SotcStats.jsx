import React, { useState } from "react";
import SideNav from "./SOTCSideNav";
import SOTCChatBotStats from "./SOTCChatBotStats";
import SOTCVoiceBotStats from "./SOTCVoiceBotStats";
import "./SotcStats.css";

function SotcStats() {
  const [activeChannel, setActiveChannel] = useState("chatbot");

  const channels = [
    { key: "chatbot", label: "ChatBot", description: "Automated assistant channel" },
    { key: "voicebot", label: "VoiceBot", description: "Voice assistant channel" },
  ];

  return (
    <div className="sotc-shell">
      <SideNav channels={channels} active={activeChannel} onSelect={setActiveChannel} />
      <main className="sotc-main">
        {activeChannel === "chatbot" && <SOTCChatBotStats />}
        {activeChannel === "voicebot" && <SOTCVoiceBotStats />}
      </main>
    </div>
  );
}

export default SotcStats;
