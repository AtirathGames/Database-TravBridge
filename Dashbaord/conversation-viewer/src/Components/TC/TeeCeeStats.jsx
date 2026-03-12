import React, { useState } from "react";
import SideNav from "./SideNav";
import ChatBotStats from "./ChatBotStats";
import VoiceBotStats from "./VoiceBotStats";
import "./TeeCeeStats.css";

function TeeCeeStats() {
  const [activeChannel, setActiveChannel] = useState("chatbot");

  const channels = [
    { key: "chatbot", label: "ChatBot", description: "Automated assistant channel" },
    { key: "voicebot", label: "VoiceBot", description: "Automated assistant channel" },
  ];

  return (
    <div className="teecee-shell">
      <SideNav channels={channels} active={activeChannel} onSelect={setActiveChannel} />
      <main className="teecee-main">
        {activeChannel === "chatbot" && <ChatBotStats />}
        {activeChannel === "voicebot" &&<VoiceBotStats />}
      </main>
    </div>
  );
}

export default TeeCeeStats;
