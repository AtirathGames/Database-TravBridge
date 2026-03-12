// import React, { useState } from "react";
// import SideNav from "./SOTCSideNav";
// import SOTCChatBotStats from "./SOTCChatBotStats";

// function SotcStats() {
//   const [activeChannel, setActiveChannel] = useState("chatbot");

//   const channels = [
//     { key: "chatbot", label: "ChatBot", description: "Automated assistant channel" },
//   ];

//   const styles = {
//     shell: { display: "flex", minHeight: "100vh", backgroundColor: "#ED1C24d9" },
//     main: { flex: 1 },
//   };

//   return (
//     <div style={styles.shell}>
//       <SideNav channels={channels} active={activeChannel} onSelect={setActiveChannel} />
//       <main style={styles.main}>
//         {activeChannel === "chatbot" && <SOTCChatBotStats />}
//       </main>
//     </div>
//   );
// }

// export default SotcStats;
