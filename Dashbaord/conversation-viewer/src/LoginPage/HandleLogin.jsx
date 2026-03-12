import { firebaseAnalytics } from "../../firebaseConfig";
import firebaseConstants from "../../utils/firebaseConstants";
import { logEventToBackend } from "../../utils/LogFirebaseEventAPI";

export const HandleLogin = async (email, mobile, navigate, setPasswordError) => {
    const eventData = firebaseConstants.EVENTS.OPEN_NEW_CHAT;
    console.log("eventData",eventData);
    await logEventToBackend(eventData);

   firebaseAnalytics.logEvent(firebaseConstants.EVENTS.OPEN_NEW_CHAT);
    const requestBody = {
        email: email,
        mobile: mobile
    };

    console.log('Request Body:', JSON.stringify(requestBody));

    const login_api = process.env.REACT_APP_API_URL + 'chat/langchain/login/';

    fetch(login_api, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestBody),
    })
        .then(async (response) => {
            const data = await response.json();
            console.log('Login Response:', data);

            if (response.status === 200) {
                console.log("User logged in with an active session");
                const firstConversationId = Object.keys(data.conversation || {})[0];
                if (!firstConversationId) {
                    console.error("No active conversation found.");
                    setPasswordError("No active conversation found.");
                    return;
                }
                const chatData = data.conversation[firstConversationId];
                // Convert "conversation" array properly
                const parsedChatHistory = chatData.conversation.map((msg) => ({
                    content: msg.content,  // Using "content" instead of "text"
                    role: msg.role === "user" ? "user" : "assistant", // Correct sender categorization
                    message_id: msg.message_id,
                    timestamp: Date.now(),
                    images: msg.url_list || [],
                    packageId: msg.packageId || null
                }));
                if (data.user_id) { 
                    const eventData =firebaseConstants.EVENTS.LOGIN_SUCCESS(data.user_id);
                    await logEventToBackend(eventData);
                    firebaseAnalytics.logEvent(firebaseConstants.EVENTS.LOGIN_SUCCESS(data.user_id));
                 }

                navigate(`/chat/${firstConversationId}`, {
                    replace: true,  // Replace the current entry in history
                    state: {
                        sessions: {
                            conversationId: firstConversationId,
                            chat_modified: chatData.chat_modified || new Date().toISOString(),
                            parsedChatHistory: parsedChatHistory  // Correctly formatted conversation
                        },
                        packages_saved: data.packages_saved,
                        user_id: data.user_id,
                        session_id: data.session_id,
                        chat_id: firstConversationId,
                        passWord: mobile,
                        user_name: data.user_name,
                        chat_name: data.chat_name,
                        assistant_response: parsedChatHistory.length > 0
                            ? parsedChatHistory[parsedChatHistory.length - 1].content
                            : "Welcome back to Thomas Cook. How can I assist you?"
                    }
                });

            } else if (response.status === 201) {
                console.log("User logged in with a new session");
                console.log("assistant_response", data.assistant_response);

                const eventData = firebaseConstants.EVENTS.NEW_CHAT_SUCCESS(data.chat_id)
                console.log("eventData2", eventData);
                await logEventToBackend(eventData);

                firebaseAnalytics.logEvent(
                    firebaseConstants.EVENTS.NEW_CHAT_SUCCESS(data.chat_id)
                  );

                navigate(`/chat/${data.chat_id}`, {
                    replace: true,
                    state: {
                        sessions: {
                            conversationId: data.chat_id,
                            chat_modified: new Date().toISOString(),
                            parsedChatHistory: [
                                {
                                    content: data.assistant_response ||
                                        "Hi, welcome to Thomas Cook. How can I assist you today?",
                                    role: "assistant",
                                    timestamp: Date.now(),
                                }
                            ] // ✅ Ensure the assistant's response is set here
                        },
                        user_id: data.user_id,
                        session_id: data.session_id,
                        chat_id: data.chat_id,
                        // passWord: password,
                        user_name: data.user_name,
                        chat_name: data.chat_name,
                        assistant_response: data.assistant_response ||
                            "Hi, welcome to Thomas Cook. How can I assist you today?",
                    }
                });
            }

            else if (response.status === 404) {
                console.log('User Not Found:', data);
                setPasswordError('User Not Found');
            } else if (response.status === 401) {
                console.log('Incorrect Password:', data);
                setPasswordError('Incorrect Password');
            } else if (response.status === 500) {
                console.log('Internal Server Error:', data);
                setPasswordError('Internal Server Error');
            } else {
                throw new Error(`Unexpected status code: ${response.status}`);
            }
        })
        .catch((error) => {
            console.error('Error:', error);
            setPasswordError('An unexpected error occurred.');
        });
};
