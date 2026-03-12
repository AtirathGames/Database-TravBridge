// // src/DailyBatchStats.js
// import React, { useEffect, useState } from "react";
// import axios from "axios";
// import { useNavigate } from "react-router-dom";

// const DailyBatchStats = () => {
//   const [stats, setStats] = useState([]);
//   const [error, setError] = useState("");
//   const [currentPage, setCurrentPage] = useState(1);
//   const pageSize = 5; // Adjust page size as needed
//   const navigate = useNavigate();

//   const fetchStats = async () => {
//     try {
//       const response = await axios.post("http://34.47.221.73:3001/v1/daily_batch_stats");
//       if (response.data.status === "success") {
//         // Sort data so that latest date is first
//         const sorted = [...response.data.data].sort(
//           (a, b) => new Date(b.date) - new Date(a.date)
//         );
//         setStats(sorted);
//       } else {
//         setError(response.data.message || "Failed to fetch stats");
//       }
//     } catch (err) {
//       console.error("Error fetching batch stats:", err);
//       setError("Could not connect to server");
//     }
//   };

//   useEffect(() => {
//     fetchStats();
//   }, []);

//   // Calculate total pages
//   const totalPages = Math.ceil(stats.length / pageSize);
//   const indexOfLast = currentPage * pageSize;
//   const indexOfFirst = indexOfLast - pageSize;
//   const currentStats = stats.slice(indexOfFirst, indexOfLast);

//   const handlePrevPage = () => {
//     if (currentPage > 1) setCurrentPage(currentPage - 1);
//   };

//   const handleNextPage = () => {
//     if (currentPage < totalPages) setCurrentPage(currentPage + 1);
//   };

//   return (
//     <div
//       style={{
//         padding: "40px",
//         backgroundColor: "#0056B3",
//         minHeight: "100vh",
//         color: "#F5F5F5",
//       }}
//     >
//       <button
//         style={{
//           marginBottom: "20px",
//           padding: "8px 16px",
//           borderRadius: "8px",
//           border: "none",
//           backgroundColor: "#0044a3",
//           color: "white",
//           cursor: "pointer",
//         }}
//         onClick={() => navigate(-1)}
//       >
//         ← Back
//       </button>

//       <h1
//         style={{
//           fontSize: "28px",
//           fontWeight: "bold",
//           color: "#f5f5f5",
//           marginBottom: "20px",
//         }}
//       >
//         Daily Batch Processing Stats
//       </h1>

//       {error && <p style={{ color: "red", marginTop: "20px" }}>{error}</p>}

//       {currentStats.map((entry, index) => (
//         <div
//           key={index}
//           style={{
//             backgroundColor: "#f8fafc",
//             color: "#000",
//             padding: "15px",
//             margin: "15px 0",
//             borderRadius: "8px",
//           }}
//         >
//           <strong>Date:</strong> {entry.date}
//           <ul style={{ paddingLeft: "20px", marginTop: "10px" }}>
//             <li>
//               <strong>Processed:</strong> {entry.stats.processed}
//             </li>
//             <li>
//               <strong>Failed:</strong> {entry.stats.failed}
//             </li>
//             <li>
//               <strong>Unique Packages:</strong> {entry.stats.uniquePackages}
//             </li>
//             <li>
//               <strong>Summary Generated:</strong> {entry.stats.generated}
//             </li>
//             <li>
//               <strong>Summary Generation Failed:</strong>{" "}
//               {entry.stats.generationFailed}
//             </li>
//           </ul>
//         </div>
//       ))}

//       {stats.length > pageSize && (
//         <div
//           style={{
//             display: "flex",
//             justifyContent: "center",
//             alignItems: "center",
//             gap: "10px",
//             marginTop: "20px",
//           }}
//         >
//           <button
//             style={{
//               padding: "8px 16px",
//               borderRadius: "8px",
//               border: "none",
//               backgroundColor: "#0044a3",
//               color: "white",
//               cursor: "pointer",
//             }}
//             onClick={handlePrevPage}
//             disabled={currentPage === 1}
//           >
//             Prev
//           </button>
//           <span>
//             Page {currentPage} of {totalPages}
//           </span>
//           <button
//             style={{
//               padding: "8px 16px",
//               borderRadius: "8px",
//               border: "none",
//               backgroundColor: "#0044a3",
//               color: "white",
//               cursor: "pointer",
//             }}
//             onClick={handleNextPage}
//             disabled={currentPage === totalPages}
//           >
//             Next
//           </button>
//         </div>
//       )}
//     </div>
//   );
// };

// export default DailyBatchStats;
