// import React, { useState } from "react";
// import SideNav from "./SideNav";
// import ChatBotStats from "./ChatBotStats";

// function TeeCeeStats() {
//   const [activeChannel, setActiveChannel] = useState("chatbot");

//   const channels = [
//     { key: "chatbot", label: "ChatBot", description: "Automated assistant channel" },
//   ];

//   const styles = {
//     shell: { display: "flex", minHeight: "100vh", backgroundColor: "#0056B3" },
//     main: { flex: 1 },
//   };

//   return (
//     <div style={styles.shell}>
//       <SideNav channels={channels} active={activeChannel} onSelect={setActiveChannel} />
//       <main style={styles.main}>
//         {activeChannel === "chatbot" && <ChatBotStats />}
//       </main>
//     </div>
//   );
// }

// export default TeeCeeStats;
