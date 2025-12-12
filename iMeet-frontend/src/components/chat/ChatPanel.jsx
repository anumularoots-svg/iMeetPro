import React, {
  useState,
  useEffect,
  useRef,
  useCallback,
  useMemo,
} from "react";
import {
  Box,
  Typography,
  IconButton,
  TextField,
  Avatar,
  Chip,
  Paper,
  InputAdornment,
  CircularProgress,
  Alert,
  Collapse,
  Badge,
  Popover,
  Grid,
  Tooltip,
  Divider,
  Button,
  List,
  ListItem,
} from "@mui/material";
import {
  Close,
  Send,
  EmojiEmotions,
  AttachFile,
  MoreVert,
  People,
  Public,
  Lock,
  Chat as ChatIcon,
  InsertDriveFile,
  Image,
  VideoFile,
  AudioFile,
  Refresh,
  CloudSync,
  SyncProblem,
  Download,
  GetApp,
  FiberManualRecord,
  CheckCircle,
  Search,
  ExpandLess,
  ExpandMore,
} from "@mui/icons-material";
import { useMeeting } from "../../hooks/useMeeting";
import { MESSAGE_TYPES, STORAGE_KEYS } from "../../utils/constants";
import cacheChatService from "../../services/cache-chat";

// Emoji data
const EMOJI_CATEGORIES = {
  "Smileys & People": [
    "😀", "😃", "😄", "😁", "😆", "😅", "😂", "🤣", "😊", "😇",
    "🙂", "🙃", "😉", "😌", "😍", "🥰", "😘", "😗", "😙", "😚",
    "😋", "😛", "😝", "😜", "🤪", "🤨", "🧐", "🤓", "😎", "🤩",
    "🥳", "😏", "😒", "😞", "😔", "😟", "😕", "🙁", "☹️", "😣",
  ],
  "Animals & Nature": [
    "🐶", "🐱", "🐭", "🐹", "🐰", "🦊", "🐻", "🐼", "🐨", "🐯",
    "🦁", "🐮", "🐷", "🐽", "🐸", "🐵", "🙈", "🙉", "🙊", "🐒",
  ],
  "Food & Drink": [
    "🍏", "🍎", "🍐", "🍊", "🍋", "🍌", "🍉", "🍇", "🍓", "🫐",
    "🍈", "🍒", "🍑", "🥭", "🍍", "🥥", "🥝", "🍅", "🍆", "🥑",
  ],
  Activity: [
    "⚽", "🏀", "🏈", "⚾", "🥎", "🎾", "🏐", "🏉", "🥏", "🎱",
    "🪀", "🏓", "🏸", "🏒", "🏑", "🥍", "🏏", "🪃", "🥅", "⛳",
  ],
  Objects: [
    "⌚", "📱", "📲", "💻", "⌨️", "🖥️", "🖨️", "🖱️", "🖲️", "🕹️",
    "🗜️", "💽", "💾", "💿", "📀", "📼", "📷", "📸", "📹", "🎥",
  ],
};

const ChatPanel = ({
  isOpen,
  isChatOpen,
  onClose,
  className = "",
  meetingId,
  participants = [],
  currentUser,
  isHost = false,
  chatPermissions = {
    canSendMessages: true,
    canUploadFiles: true,
  },
  meetingSettings = {},
}) => {
  const actuallyOpen = isOpen || isChatOpen;

  const [newMessage, setNewMessage] = useState("");
  const [isTyping, setIsTyping] = useState(false);
  const [typingUsers, setTypingUsers] = useState(new Set());
  const [showEmojiPicker, setShowEmojiPicker] = useState(false);
  const [emojiAnchorEl, setEmojiAnchorEl] = useState(null);
  const [selectedEmojiCategory, setSelectedEmojiCategory] =
    useState("Smileys & People");
  const [unreadCount, setUnreadCount] = useState(0);
  const [isAtBottom, setIsAtBottom] = useState(true);
  const [cacheMessages, setCacheMessages] = useState([]);
  const [showParticipants, setShowParticipants] = useState(true);
  const [messageType, setMessageType] = useState("public");
  const [searchTerm, setSearchTerm] = useState("");
  const [filteredMessages, setFilteredMessages] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [chatStats, setChatStats] = useState({
    totalMessages: 0,
    storageType: "cache_only",
  });
  const [isChatInitialized, setIsChatInitialized] = useState(false);
  const [uploadingFiles, setUploadingFiles] = useState([]);
  const [lastMessageTimestamp, setLastMessageTimestamp] = useState(null);
  const [syncStatus, setSyncStatus] = useState("connected");
  const [messagesSyncing, setMessagesSyncing] = useState(false);
  const [lastSeenMessageId, setLastSeenMessageId] = useState(null);
  const [lastSeenTimestamp, setLastSeenTimestamp] = useState(null);
  const [chatVisible, setChatVisible] = useState(false);
  const [showPrivateChatToggle, setShowPrivateChatToggle] = useState(true);
  const [privateChatEnabled, setPrivateChatEnabled] = useState(
    meetingSettings.privateChatEnabled ?? true
  );
  const [messageRecipient, setMessageRecipient] = useState("everyone");
  const [showRecipientSelector, setShowRecipientSelector] = useState(false);
  const [selectedParticipants, setSelectedParticipants] = useState([]);
  const [recipientSearchTerm, setRecipientSearchTerm] = useState("");

  const messagesEndRef = useRef(null);
  const chatContainerRef = useRef(null);
  const inputRef = useRef(null);
  const fileInputRef = useRef(null);
  const typingTimeoutRef = useRef(null);
  const messagePollingRef = useRef(null);
  const lastPollRef = useRef(Date.now());
  const messagesSentRef = useRef(new Set());
  const intersectionObserverRef = useRef(null);
  const eventListenerRef = useRef(null);

  // Callbacks for recipient selector
  const handleRecipientToggle = useCallback(() => {
    setShowRecipientSelector((prev) => !prev);
  }, []);

  const handleEveryone = useCallback((e) => {
    e.preventDefault();
    e.stopPropagation();
    setMessageRecipient("everyone");
    setSelectedParticipants([]);
    setShowRecipientSelector(false);
  }, []);

  const handleSelectParticipant = useCallback((participantId) => {
    setSelectedParticipants((prev) => {
      if (prev.includes(participantId)) {
        return prev.filter((id) => id !== participantId);
      } else {
        return [...prev, participantId];
      }
    });
  }, []);

  const handleApplySelection = useCallback(() => {
    setMessageRecipient(selectedParticipants);
    setShowRecipientSelector(false);
    setRecipientSearchTerm("");
  }, [selectedParticipants]);

  const handleCancelSelection = useCallback(() => {
    setSelectedParticipants([]);
    setShowRecipientSelector(false);
    setRecipientSearchTerm("");
  }, []);

  const scrollToBottom = useCallback(() => {
    if (messagesEndRef.current && isAtBottom) {
      messagesEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [isAtBottom]);

  useEffect(() => {
    setChatVisible(actuallyOpen);

    if (actuallyOpen) {
      setTimeout(() => {
        markMessagesAsRead();
      }, 500);
    }
  }, [actuallyOpen]);

  const markMessagesAsRead = useCallback(() => {
    if (!chatVisible || filteredMessages.length === 0) return;

    const lastMessage = filteredMessages[filteredMessages.length - 1];
    if (lastMessage && (lastMessage.id || lastMessage.timestamp)) {
      const messageId = lastMessage.id || lastMessage.timestamp;
      const timestamp = lastMessage.timestamp;

      if (lastSeenMessageId !== messageId || lastSeenTimestamp !== timestamp) {
        setLastSeenMessageId(messageId);
        setLastSeenTimestamp(timestamp);
        setUnreadCount(0);

        if (meetingId) {
          localStorage.setItem(
            `chat_last_seen_${meetingId}`,
            JSON.stringify({
              messageId,
              timestamp,
              userId: currentUser?.id,
            })
          );
        }
      }
    }
  }, [
    chatVisible,
    filteredMessages,
    lastSeenMessageId,
    lastSeenTimestamp,
    meetingId,
    currentUser?.id,
  ]);

  useEffect(() => {
    if (meetingId && currentUser?.id) {
      const stored = localStorage.getItem(`chat_last_seen_${meetingId}`);
      if (stored) {
        try {
          const { messageId, timestamp, userId } = JSON.parse(stored);
          if (userId === currentUser.id) {
            setLastSeenMessageId(messageId);
            setLastSeenTimestamp(timestamp);
          }
        } catch (error) {
          console.warn("Failed to parse stored last seen message:", error);
        }
      }
    }
  }, [meetingId, currentUser?.id]);

  const calculateUnreadCount = useCallback(() => {
    if (!lastSeenMessageId && !lastSeenTimestamp) {
      const unreadFromOthers = filteredMessages.filter(
        (msg) => msg.userId !== currentUser?.id
      ).length;
      return unreadFromOthers;
    }

    const lastSeenIndex = filteredMessages.findIndex(
      (msg) =>
        (msg.id && msg.id === lastSeenMessageId) ||
        msg.timestamp === lastSeenTimestamp
    );

    if (lastSeenIndex === -1) {
      const unreadFromOthers = filteredMessages.filter(
        (msg) => msg.userId !== currentUser?.id
      ).length;
      return unreadFromOthers;
    }

    const messagesAfterLastSeen = filteredMessages.slice(lastSeenIndex + 1);
    const unreadFromOthers = messagesAfterLastSeen.filter(
      (msg) => msg.userId !== currentUser?.id
    ).length;

    return unreadFromOthers;
  }, [filteredMessages, lastSeenMessageId, lastSeenTimestamp, currentUser?.id]);

  useEffect(() => {
    if (!chatVisible) {
      const newUnreadCount = calculateUnreadCount();
      setUnreadCount(newUnreadCount);
    } else {
      setTimeout(() => {
        markMessagesAsRead();
      }, 1000);
    }
  }, [filteredMessages, chatVisible, calculateUnreadCount, markMessagesAsRead]);

  useEffect(() => {
    if (!messagesEndRef.current || !chatContainerRef.current) return;

    const observer = new IntersectionObserver(
      (entries) => {
        const [entry] = entries;
        setIsAtBottom(entry.isIntersecting);

        if (entry.isIntersecting && chatVisible) {
          setTimeout(markMessagesAsRead, 100);
        }
      },
      {
        root: chatContainerRef.current,
        threshold: 0.1,
      }
    );

    observer.observe(messagesEndRef.current);
    intersectionObserverRef.current = observer;

    return () => {
      if (intersectionObserverRef.current) {
        intersectionObserverRef.current.disconnect();
      }
    };
  }, [chatVisible, markMessagesAsRead]);

  const safeParseFileData = useCallback((fileData) => {
    if (!fileData) return null;

    if (typeof fileData === "object") {
      return fileData;
    }

    if (typeof fileData === "string") {
      try {
        return JSON.parse(fileData);
      } catch (e) {
        console.warn("Failed to parse fileData JSON:", e);
        return null;
      }
    }

    return null;
  }, []);

  const extractFileInfoFromMessage = useCallback((message) => {
    if (!message || typeof message !== "string") return null;

    const patterns = [
      /📎\s*Shared a file:\s*(.+?)\s*\(([0-9.]+)\s*(KB|MB|GB)\)/i,
      /📷\s*Shared an image:\s*(.+?)\s*\(([0-9.]+)\s*(KB|MB|GB)\)/i,
      /📎\s*(.+?)\s*\(([0-9.]+)\s*(KB|MB|GB)\)/i,
      /Shared a file:\s*(.+?)(?:\s*\(([0-9.]+)\s*(KB|MB|GB)\))?/i,
      /Shared an image:\s*(.+?)(?:\s*\(([0-9.]+)\s*(KB|MB|GB)\))?/i,
    ];

    for (const pattern of patterns) {
      const match = message.match(pattern);
      if (match) {
        const [, fileName, size, unit] = match;
        let sizeInBytes = 0;

        if (size && unit) {
          const sizeNum = parseFloat(size);
          const unitLower = unit.toLowerCase();
          sizeInBytes =
            unitLower === "kb"
              ? sizeNum * 1024
              : unitLower === "mb"
              ? sizeNum * 1024 * 1024
              : unitLower === "gb"
              ? sizeNum * 1024 * 1024 * 1024
              : sizeNum;
        }

        return {
          name: fileName.trim(),
          size: sizeInBytes,
          type: getFileTypeFromName(fileName),
          url: null,
          isFromPreviousSession: false,
        };
      }
    }

    return null;
  }, []);

  useEffect(() => {
    if (meetingId && actuallyOpen && !isChatInitialized) {
      initializeCacheChat();
    }

    return () => {
      if (messagePollingRef.current) {
        clearInterval(messagePollingRef.current);
      }
      if (typingTimeoutRef.current) {
        clearTimeout(typingTimeoutRef.current);
      }
      if (intersectionObserverRef.current) {
        intersectionObserverRef.current.disconnect();
      }
    };
  }, [meetingId, actuallyOpen, isChatInitialized]);

 const initializeCacheChat = async () => {
  try {
    setIsLoading(true);
    console.log("🎬 Initializing cache-only chat for meeting:", meetingId);

    // NEW: Pass userId and isHost for private message filtering
    const startResult = await cacheChatService.startMeetingChat(
      meetingId,
      currentUser?.id,
      isHost
    );
      if (startResult.success) {
        setIsChatInitialized(true);
        setSyncStatus("connected");
        console.log("✅ Cache-only chat initialized");
        await loadCacheMessages();
      } else {
        throw new Error(startResult.error || "Failed to initialize chat");
      }
    } catch (error) {
      console.error("❌ Failed to initialize cache chat:", error);
      setSyncStatus("error");
      setError({
        type: "error",
        message: "Failed to initialize chat: " + error.message,
      });
    } finally {
      setIsLoading(false);
    }
  };

  const loadCacheMessages = async (force = false) => {
    if (!meetingId) return;

    try {
      setMessagesSyncing(true);

      console.log("📥 Loading messages with context:", {
        meetingId,
        currentUserId: currentUser?.id,
        isHost,
        force,
      });

      const result = await cacheChatService.getChatHistory(
        meetingId,
        100,
        0,
        currentUser?.id,
        isHost
      );

      if (result.success) {
        const newMessages = result.messages || [];

        const formattedMessages = newMessages.map((msg) => {
          const formatted = cacheChatService.formatMessage(msg);

          let fileInfo = null;
          if (formatted.messageType === "file" || formatted.fileData) {
            const parsedFileData = safeParseFileData(formatted.fileData);

            if (parsedFileData) {
              fileInfo = {
                ...parsedFileData,
                url: parsedFileData.file_id
                  ? cacheChatService.createFileDownloadUrl(
                      parsedFileData.file_id
                    )
                  : parsedFileData.originalUrl || parsedFileData.url,
              };
            }
          }

          if (!fileInfo && formatted.message) {
            const extractedFileInfo = extractFileInfoFromMessage(
              formatted.message
            );
            if (extractedFileInfo) {
              const fileId = msg.file_id || formatted.file_id;
              if (fileId) {
                extractedFileInfo.url =
                  cacheChatService.createFileDownloadUrl(fileId);
                extractedFileInfo.file_id = fileId;
                extractedFileInfo.isFromPreviousSession = false;
              } else {
                extractedFileInfo.isFromPreviousSession = true;
              }
              fileInfo = extractedFileInfo;
              formatted.messageType = "file";
            }
          }

          if (fileInfo) {
            formatted.fileData = fileInfo;
            formatted.fileUrl = fileInfo.url || null;
            formatted.fileType = fileInfo.type || null;
            formatted.fileSize = fileInfo.size || 0;
            formatted.messageType = "file";
          }

          return formatted;
        });

        // setCacheMessages(formattedMessages);
        // Only update if messages have actually changed
setCacheMessages((prevMessages) => {
  // Check if messages are actually different
  if (prevMessages.length === formattedMessages.length) {
    const hasChanges = formattedMessages.some((newMsg, index) => {
      const oldMsg = prevMessages[index];
      return (
        newMsg.id !== oldMsg?.id ||
        newMsg.message !== oldMsg?.message ||
        newMsg.timestamp !== oldMsg?.timestamp
      );
    });
    
    if (!hasChanges) {
      console.log("📥 No message changes detected, skipping update");
      return prevMessages; // Return previous state to prevent re-render
    }
  }
  
  console.log("📥 Messages updated:", formattedMessages.length);
  return formattedMessages;
});

setChatStats({
  totalMessages: result.totalCount,
  currentMessages: result.count,
  storageType: "cache_only",
});
       

        if (formattedMessages.length > 0) {
          setLastMessageTimestamp(
            formattedMessages[formattedMessages.length - 1]?.timestamp
          );
        }

        setSyncStatus("connected");
        console.log("📥 Messages updated:", formattedMessages.length);
      } else {
        console.warn("Failed to load messages:", result.error);
        setSyncStatus("warning");
      }
    } catch (error) {
      console.error("❌ Error loading messages:", error);
      setSyncStatus("error");
    } finally {
      setMessagesSyncing(false);
    }
  };

  useEffect(() => {
    if (meetingId && actuallyOpen && isChatInitialized) {
      console.log("🔄 Setting up real-time sync with user context");

      if (eventListenerRef.current) {
        window.removeEventListener(
          "cacheMessagesUpdated",
          eventListenerRef.current
        );
      }

      const handleMessageUpdate = (event) => {
  const { meetingId: eventMeetingId, messages } = event.detail;
  if (eventMeetingId === meetingId) {
    console.log(
      "📨 Real-time update received:",
      messages.length,
      "messages"
    );

    const processedMessages = messages.map((msg) => {
      if (msg.messageType === "file" && msg.fileData) {
        const fileInfo = safeParseFileData(msg.fileData);
        if (fileInfo && fileInfo.file_id) {
          fileInfo.url = cacheChatService.createFileDownloadUrl(
            fileInfo.file_id
          );
          msg.fileData = fileInfo;
          msg.fileUrl = fileInfo.url;
        }
      }
      return msg;
    });

    // Only update if messages have actually changed
    setCacheMessages((prevMessages) => {
      if (prevMessages.length === processedMessages.length) {
        const hasChanges = processedMessages.some((newMsg, index) => {
          const oldMsg = prevMessages[index];
          return (
            newMsg.id !== oldMsg?.id ||
            newMsg.message !== oldMsg?.message ||
            newMsg.timestamp !== oldMsg?.timestamp
          );
        });
        
        if (!hasChanges) {
          console.log("📨 No changes in real-time update, skipping");
          return prevMessages;
        }
      }
      
      console.log("✅ Updated messages:", processedMessages.length);
      setTimeout(scrollToBottom, 50);
      return processedMessages;
    });
  }
};

      eventListenerRef.current = handleMessageUpdate;
      window.addEventListener("cacheMessagesUpdated", handleMessageUpdate);

      return () => {
        if (eventListenerRef.current) {
          window.removeEventListener(
            "cacheMessagesUpdated",
            eventListenerRef.current
          );
          eventListenerRef.current = null;
        }
      };
    }
  }, [meetingId, actuallyOpen, isChatInitialized, scrollToBottom]);
  useEffect(() => {
    if (!searchTerm.trim()) {
      setFilteredMessages(cacheMessages);
    } else {
      const filtered = cacheMessages.filter(
        (message) =>
          message.message?.toLowerCase().includes(searchTerm.toLowerCase()) ||
          message.userName?.toLowerCase().includes(searchTerm.toLowerCase())
      );
      setFilteredMessages(filtered);
    }
  }, [cacheMessages, searchTerm]);

  useEffect(() => {
    scrollToBottom();
  }, [filteredMessages, scrollToBottom]);

  const handleEmojiSelect = useCallback((emoji) => {
    setNewMessage((prev) => prev + emoji);
    setShowEmojiPicker(false);
    setEmojiAnchorEl(null);
    inputRef.current?.focus();
  }, []);

  const handleEmojiToggle = useCallback(
    (event) => {
      if (showEmojiPicker) {
        setShowEmojiPicker(false);
        setEmojiAnchorEl(null);
      } else {
        setShowEmojiPicker(true);
        setEmojiAnchorEl(event.currentTarget);
      }
    },
    [showEmojiPicker]
  );

  const handleFileSelect = useCallback(
    async (event) => {
      const files = Array.from(event.target.files);
      if (files.length === 0) return;

      console.log(
        "📁 Starting enhanced file upload process for files:",
        files.map((f) => f.name)
      );

      for (const file of files) {
        const uploadId = `file_${Date.now()}_${Math.random()
          .toString(36)
          .substr(2, 9)}`;

        try {
          setUploadingFiles((prev) => [
            ...prev,
            { file, progress: 0, id: uploadId },
          ]);

          console.log("📤 Uploading file via enhanced service:", file.name);

          // FIXED: Properly determine file privacy based on messageRecipient
          let isPrivateFile = false;
          let fileRecipients = [];

          console.log("Current messageRecipient:", messageRecipient);

          // Check if sending to host only
          if (messageRecipient === "host") {
            isPrivateFile = true;
            const hostParticipant = participants.find(
              (p) => p.isHost || p.role === "host"
            );
            if (hostParticipant) {
              const hostId = hostParticipant.user_id || hostParticipant.id;
              fileRecipients = [String(hostId)];
              console.log("🔒 Private file to HOST ONLY:", hostId);
            }
          }
          // Check if sending to selected participants
          else if (
            Array.isArray(messageRecipient) &&
            messageRecipient.length > 0
          ) {
            isPrivateFile = true;
            fileRecipients = messageRecipient.map((id) => String(id));
            console.log("🔒 Private file to SELECTED participants:", fileRecipients);
          }
          // Otherwise it's public (everyone)
          else {
            isPrivateFile = false;
            fileRecipients = [];
            console.log("🌐 Public file - sending to EVERYONE");
          }

          console.log("📎 File privacy determined:", {
            isPrivate: isPrivateFile,
            recipientCount: fileRecipients.length,
            recipients: fileRecipients,
            messageRecipient: messageRecipient,
          });

          // Call uploadFile with correct parameters
          const result = await cacheChatService.uploadFile(
            meetingId,
            file,
            currentUser?.id || "anonymous",
            currentUser?.name || currentUser?.full_name || "You",
            (progress) => {
              setUploadingFiles((prev) =>
                prev.map((uf) =>
                  uf.id === uploadId ? { ...uf, progress } : uf
                )
              );
            },
            isPrivateFile, // FIXED: Pass actual private flag
            fileRecipients // FIXED: Pass actual recipients array
          );

          if (result.success) {
            console.log("✅ File uploaded successfully");
            console.log("   Response - Is Private:", result.isPrivate);
            console.log("   Response - Recipients:", result.recipients);

            setUploadingFiles((prev) =>
              prev.map((uf) =>
                uf.id === uploadId ? { ...uf, progress: 100 } : uf
              )
            );

            setTimeout(async () => {
              setUploadingFiles((prev) =>
                prev.filter((uf) => uf.id !== uploadId)
              );
              setTimeout(scrollToBottom, 100);
            }, 1000);

            const privacyLabel = result.isPrivate
              ? ` (Private to ${result.recipients?.length || 0} recipient(s))`
              : " (Public)";

            console.log(
              `✅ File message created with correct privacy settings${privacyLabel}`
            );
          } else {
            throw new Error(result.error || "Failed to upload file");
          }
        } catch (error) {
          console.error("❌ Failed to upload file:", error);
          setError({
            type: "error",
            message: `Failed to upload ${file.name}: ${error.message}`,
          });
          setUploadingFiles((prev) =>
            prev.filter((uf) => uf.id !== uploadId)
          );
        }
      }

      // Reset file input
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    },
    [meetingId, currentUser, messageRecipient, participants, scrollToBottom]
  );

  const handleFileUpload = useCallback(() => {
    if (!chatPermissions.canUploadFiles) {
      setError({
        type: "warning",
        message: "File uploads are not allowed in this chat",
      });
      return;
    }
    fileInputRef.current?.click();
  }, [chatPermissions.canUploadFiles]);

  const handleSendMessage = useCallback(
    async (e) => {
      e.preventDefault();

      if (!newMessage.trim() || !isChatInitialized) return;

      const messageText = newMessage.trim();
      setNewMessage("");

      try {
        let recipients = [];
        let isPrivateMessage = false;

        if (messageRecipient === "host") {
          const hostParticipant = participants.find(
            (p) => p.isHost || p.role === "host"
          );
          if (hostParticipant) {
            recipients = [hostParticipant.user_id || hostParticipant.id];
            isPrivateMessage = true;
          }
        } else if (
          Array.isArray(messageRecipient) &&
          messageRecipient.length > 0
        ) {
          recipients = messageRecipient;
          isPrivateMessage = true;
        }

        const messageData = {
          meetingId: meetingId,
          userId: currentUser?.id || "anonymous",
          userName: currentUser?.name || currentUser?.full_name || "You",
          message: messageText,
          messageType: isPrivateMessage ? "private" : "text",
          isPrivate: isPrivateMessage,
          recipients: recipients,
          senderIsHost: isHost,
        };

        console.log("📤 Sending message:", {
          isPrivate: isPrivateMessage,
          recipients: recipients.length,
          sender: currentUser?.id,
        });

        const result = await cacheChatService.sendMessage(messageData);

        if (result.success) {
          console.log("✅ Message sent successfully");
          setTimeout(scrollToBottom, 100);

          if (!isHost) {
            setMessageRecipient(privateChatEnabled ? "everyone" : "everyone");
          }
        } else {
          throw new Error(result.error || "Failed to send message");
        }
      } catch (error) {
        console.error("❌ Failed to send message:", error);
        setError({
          type: "error",
          message: "Failed to send message: " + error.message,
        });
        setNewMessage(messageText);
      }
    },
    [
      newMessage,
      meetingId,
      currentUser,
      messageRecipient,
      isHost,
      privateChatEnabled,
      isChatInitialized,
      scrollToBottom,
      participants,
    ]
  );

  const handleTypingStart = useCallback(async () => {
    if (!isTyping && isChatInitialized) {
      setIsTyping(true);
      try {
        await cacheChatService.updateTypingStatus(
          meetingId,
          currentUser?.id || "anonymous",
          currentUser?.name || currentUser?.full_name || "You",
          true
        );
      } catch (error) {
        console.warn("Failed to update typing status:", error);
      }
    }

    if (typingTimeoutRef.current) {
      clearTimeout(typingTimeoutRef.current);
    }

    typingTimeoutRef.current = setTimeout(async () => {
      if (isTyping) {
        setIsTyping(false);
        try {
          await cacheChatService.updateTypingStatus(
            meetingId,
            currentUser?.id || "anonymous",
            currentUser?.name || currentUser?.full_name || "You",
            false
          );
        } catch (error) {
          console.warn("Failed to stop typing status:", error);
        }
      }
    }, 3000);
  }, [isTyping, isChatInitialized, meetingId, currentUser]);

  const handleKeyPress = useCallback(
    (e) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        handleSendMessage(e);
      } else {
        handleTypingStart();
      }
    },
    [handleSendMessage, handleTypingStart]
  );

  const handleManualRefresh = useCallback(async () => {
    console.log("🔄 Manual refresh triggered via enhanced service");
    setMessagesSyncing(true);
    try {
      await cacheChatService.forceRefresh(meetingId);
      await loadCacheMessages(true);
      setSyncStatus("connected");
    } catch (error) {
      console.error("❌ Manual refresh failed:", error);
      setSyncStatus("error");
    } finally {
      setMessagesSyncing(false);
    }
  }, [meetingId]);

  const formatMessageTime = useCallback((timestamp) => {
    try {
      const date = new Date(timestamp);
      return date.toLocaleTimeString([], {
        hour: "numeric",
        minute: "2-digit",
        hour12: true,
      });
    } catch (error) {
      return "";
    }
  }, []);

  const getMessageSender = useCallback(
    (message) => {
      if (message?.userId === currentUser?.id) {
        return "You";
      }
      return message?.userName || message?.user_name || "Anonymous";
    },
    [currentUser?.id]
  );

  const handleClose = useCallback(() => {
    if (typeof onClose === "function") {
      onClose();
    }
  }, [onClose]);

  const getInitials = useCallback((name) => {
    if (!name) return "?";
    return name
      .split(" ")
      .map((n) => n[0])
      .join("")
      .toUpperCase()
      .slice(0, 2);
  }, []);

  const getParticipantColor = useCallback((userId) => {
    const colors = [
      "#e91e63",
      "#2196f3",
      "#4caf50",
      "#9c27b0",
      "#ff9800",
      "#f44336",
      "#3f51b5",
      "#009688",
    ];
    const index = parseInt(userId?.toString() || "0") % colors.length;
    return colors[index];
  }, []);

  const getFileIcon = useCallback((fileType) => {
    if (fileType?.startsWith("image/"))
      return <Image sx={{ color: "#4caf50" }} />;
    if (fileType?.startsWith("video/"))
      return <VideoFile sx={{ color: "#f44336" }} />;
    if (fileType?.startsWith("audio/"))
      return <AudioFile sx={{ color: "#ff9800" }} />;
    if (fileType?.includes("pdf"))
      return <InsertDriveFile sx={{ color: "#f44336" }} />;
    return <InsertDriveFile sx={{ color: "rgba(255, 255, 255, 0.7)" }} />;
  }, []);

  const getFileTypeFromName = useCallback((fileName) => {
    if (!fileName) return "application/octet-stream";

    const extension = fileName.split(".").pop()?.toLowerCase();
    const typeMap = {
      jpg: "image/jpeg",
      jpeg: "image/jpeg",
      png: "image/png",
      gif: "image/gif",
      pdf: "application/pdf",
      doc: "application/msword",
      txt: "text/plain",
      mp3: "audio/mpeg",
      wav: "audio/wav",
      mp4: "video/mp4",
      avi: "video/x-msvideo",
      zip: "application/zip",
      json: "application/json",
      csv: "text/csv",
    };

    return typeMap[extension] || "application/octet-stream";
  }, []);

  const getFileExtension = useCallback((fileName) => {
    if (!fileName) return "";
    const extension = fileName.split(".").pop();
    return extension ? extension.toUpperCase() : "";
  }, []);

  const formatFileSize = useCallback((bytes) => {
    if (bytes === 0) return "0 Bytes";
    const k = 1024;
    const sizes = ["Bytes", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
  }, []);

  const handleFileClick = useCallback((fileData, message) => {
    if (!fileData) {
      setError({
        type: "warning",
        message: "File information is not available",
      });
      return;
    }

    console.log("🖱️ File clicked:", fileData);

    if (fileData.url && !fileData.isFromPreviousSession) {
      console.log("📥 Downloading file from:", fileData.url);

      if (fileData.type?.startsWith("image/")) {
        const newWindow = window.open(fileData.url, "_blank");
        if (!newWindow) {
          window.location.href = fileData.url;
        }
      } else {
        const link = document.createElement("a");
        link.href = fileData.url;
        link.download = fileData.name || "download";
        link.target = "_blank";
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
      }
    } else if (fileData.isFromPreviousSession) {
      setError({
        type: "info",
        message: `"${
          fileData.name || "File"
        }" was shared in a previous session and is no longer available for download.`,
      });
    } else {
      const fileId = fileData.file_id || message?.file_id;
      if (fileId) {
        const downloadUrl = cacheChatService.createFileDownloadUrl(fileId);
        console.log(
          "🔗 Attempting download with constructed URL:",
          downloadUrl
        );

        const link = document.createElement("a");
        link.href = downloadUrl;
        link.download = fileData.name || "download";
        link.target = "_blank";
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
      } else {
        setError({
          type: "warning",
          message: "File is not available for download",
        });
      }
    }
  }, []);

  useEffect(() => {
    const handleClickOutside = (event) => {
      if (emojiAnchorEl && !emojiAnchorEl.contains(event.target)) {
        setShowEmojiPicker(false);
        setEmojiAnchorEl(null);
      }
    };

    if (showEmojiPicker) {
      document.addEventListener("mousedown", handleClickOutside);
      return () =>
        document.removeEventListener("mousedown", handleClickOutside);
    }
  }, [showEmojiPicker, emojiAnchorEl]);

  if (!actuallyOpen) {
    return null;
  }

  const getSyncStatusIcon = () => {
    switch (syncStatus) {
      case "connected":
        return <FiberManualRecord sx={{ color: "#4caf50", fontSize: 12 }} />;
      case "warning":
        return <FiberManualRecord sx={{ color: "#ff9800", fontSize: 12 }} />;
      case "error":
        return <FiberManualRecord sx={{ color: "#f44336", fontSize: 12 }} />;
      default:
        return <FiberManualRecord sx={{ color: "#666", fontSize: 12 }} />;
    }
  };

  const activeParticipants = participants.filter((p) => p.isActive !== false);

  const availableParticipants = useMemo(
    () =>
      activeParticipants.filter(
        (p) => !p.isHost && p.user_id !== currentUser?.id
      ),
    [activeParticipants, currentUser?.id]
  );

  const filteredParticipants = useMemo(() => {
    return availableParticipants.filter((p) => {
      const displayName = p.displayName || p.name || p.full_name || "";
      return displayName
        .toLowerCase()
        .includes(recipientSearchTerm.toLowerCase());
    });
  }, [availableParticipants, recipientSearchTerm]);

  return (
    <>
      <input
        ref={fileInputRef}
        type="file"
        multiple
        accept="*/*"
        style={{ display: "none" }}
        onChange={handleFileSelect}
      />

      <Box
        sx={{
          position: "fixed",
          top: 0,
          right: 0,
          bottom: 0,
          width: { xs: "100vw", sm: "400px", md: "450px" },
          height: "90vh",
          backgroundColor: "#ffffff",
          borderRadius: { xs: 0, sm: "20px" },
          boxShadow: "-2px 0 20px rgba(0, 0, 0, 0.1)",
          display: "flex",
          flexDirection: "column",
          zIndex: 1300,
          borderLeft: { xs: "none", sm: "1px solid #e5e7eb" },
          overflow: "hidden",
        }}
      >
        <Box
          sx={{
            p: 2,
            backgroundColor: "#ffffff",
            borderBottom: "1px solid #f3f4f6",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            minHeight: "50px",
          }}
        >
          <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
            <Badge
              badgeContent={unreadCount}
              color="error"
              max={99}
              sx={{
                "& .MuiBadge-badge": {
                  backgroundColor: "#ef4444",
                  color: "white",
                  fontWeight: 600,
                  fontSize: "0.75rem",
                  minWidth: "20px",
                  height: "20px",
                  borderRadius: "10px",
                },
              }}
            >
              <Box
                sx={{
                  width: 48,
                  height: 48,
                  backgroundColor: "#3b82f6",
                  borderRadius: 2,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                }}
              >
                <ChatIcon sx={{ color: "white", fontSize: 24 }} />
              </Box>
            </Badge>
            <Box>
              <Typography
                variant="h6"
                sx={{
                  fontWeight: 700,
                  fontSize: "1.125rem",
                  color: "#111827",
                  mb: 0.5,
                }}
              >
                Chat
                {unreadCount > 0 && (
                  <Chip
                    label={`${unreadCount} new`}
                    size="small"
                    sx={{
                      ml: 1,
                      backgroundColor: "#ef4444",
                      color: "white",
                      fontSize: "0.7rem",
                      fontWeight: 600,
                      height: "20px",
                    }}
                  />
                )}
              </Typography>
              <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                {getSyncStatusIcon()}
                <Typography
                  variant="body2"
                  sx={{ color: "#6b7280", fontSize: "0.875rem" }}
                >
                  • {participants.length} participant(s)
                </Typography>
              </Box>
            </Box>
          </Box>

          <IconButton
            onClick={handleClose}
            sx={{
              width: 40,
              height: 40,
              backgroundColor: "#f3f4f6",
              "&:hover": {
                backgroundColor: "#e5e7eb",
                transform: "scale(1.05)",
              },
              transition: "all 0.2s ease",
            }}
          >
            <Close sx={{ fontSize: 20, color: "#6b7280" }} />
          </IconButton>
        </Box>

        {error && (
          <Box sx={{ px: 3, pb: 2 }}>
            <Alert
              severity={error.type || "error"}
              onClose={() => setError(null)}
              sx={{
                borderRadius: 2,
                "& .MuiAlert-message": {
                  fontSize: "0.875rem",
                },
              }}
            >
              {error.message}
            </Alert>
          </Box>
        )}

        <Box
          ref={chatContainerRef}
          sx={{
            flex: 1,
            overflowY: "auto",
            px: 3,
            py: 1,
            backgroundColor: "#ffffff",
            "&::-webkit-scrollbar": { width: "6px" },
            "&::-webkit-scrollbar-track": { background: "#f3f4f6" },
            "&::-webkit-scrollbar-thumb": {
              background: "#d1d5db",
              borderRadius: "3px",
              "&:hover": { background: "#9ca3af" },
            },
          }}
        >
          {isLoading ? (
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                justifyContent: "center",
                height: "100%",
                gap: 2,
                py: 4,
              }}
            >
              <CircularProgress sx={{ color: "#3b82f6" }} />
              <Typography variant="body2" sx={{ color: "#6b7280" }}>
                Connecting to chat...
              </Typography>
            </Box>
          ) : filteredMessages.length === 0 ? (
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                justifyContent: "center",
                height: "100%",
                color: "#9ca3af",
                p: 4,
                textAlign: "center",
              }}
            >
              <ChatIcon sx={{ fontSize: 64, mb: 2, opacity: 0.3 }} />
              <Typography variant="h6" sx={{ mb: 1, color: "#6b7280" }}>
                No messages yet
              </Typography>
              <Typography variant="body2" sx={{ color: "#9ca3af" }}>
                Start the conversation!
              </Typography>
            </Box>
          ) : (
            <Box sx={{ py: 2 }}>
     {filteredMessages.map((message) => {
  if (!message || (!message.id && !message.timestamp)) {
    return null;
  }

  const messageKey =
    message.id ||
    `${message.timestamp}_${message.message?.slice(0, 10)}`;
  
  // Ensure both are strings for proper comparison
  const messageUserId = String(message.userId || message.user_id || "");
  const currentUserId = String(currentUser?.id || "");
  const isOwnMessage = messageUserId === currentUserId && currentUserId !== "";
                // const isOwnMessage = message.userId === currentUser?.id;
                const sender = getMessageSender(message);
                const isPrivateMessage =
                  message.isPrivate === true ||
                  message.messageType === "private";

                const isFileMessage =
                  message.messageType === "file" ||
                  message.fileData ||
                  (message.message &&
                    (message.message.includes("📎") ||
                      message.message.includes("📷") ||
                      message.message.toLowerCase().includes("shared a file") ||
                      message.message
                        .toLowerCase()
                        .includes("shared an image")));

                return (
                  <Box
  key={messageKey}
  sx={{
    mb: 2,
    display: "flex",
    flexDirection: isOwnMessage ? "row-reverse" : "row",
    alignItems: "flex-end",
    gap: 1.5,
    width: "100%",
  }}
>
                {!isOwnMessage && (
  <Avatar
    sx={{
      width: 36,
      height: 36,
      fontSize: "0.8rem",
      backgroundColor: getParticipantColor(message.userId),
      fontWeight: 600,
      flexShrink: 0,
    }}
  >
    {getInitials(message.userName)}
  </Avatar>
)}

                   <Box
  sx={{
    display: "flex",
    flexDirection: "column",
    maxWidth: isOwnMessage ? "75%" : "calc(100% - 52px)",
    alignItems: isOwnMessage ? "flex-end" : "flex-start",
  }}
>
                     <Box
  sx={{
    display: "flex",
    alignItems: "center",
    gap: 1,
    mb: 0.5,
    flexDirection: isOwnMessage ? "row-reverse" : "row",
  }}
>
  {/* Only show sender name for other users' messages */}
  {!isOwnMessage && (
    <Typography
      variant="caption"
      sx={{
        fontWeight: 600,
        color: "#374151",
        fontSize: "0.8125rem",
      }}
    >
      {sender}
    </Typography>
  )}

  {isPrivateMessage &&
  (() => {
    const recipients = message.recipients || [];
    const senderId = String(message.userId || message.user_id || "");
    const senderName = message.userName || message.user_name || "Someone";
    const currentUserId = String(currentUser?.id || "");
    
    // Determine if current user is the sender
    const isSender = currentUserId !== "" && senderId === currentUserId;
    
    const getPrivateLabel = () => {
      if (isSender) {
        // SENDER VIEW: Show "Private to [recipient name]"
        if (recipients.length === 0) return "Private";
        if (recipients.length === 1) {
          const recipientId = String(recipients[0]);
          const recipient = participants.find(
            (p) => String(p.user_id || p.id) === recipientId
          );
          const recipientName = recipient?.displayName || 
                               recipient?.name || 
                               recipient?.full_name || 
                               "Participant";
          return `Private to ${recipientName}`;
        }
        return `Private to ${recipients.length} participants`;
      } else {
        // RECEIVER VIEW: Show "Private from [sender name]"
        // ✅ FIX: Use senderName directly (already extracted from message)
        return `Private from ${senderName}`;
      }
    };

    // Different colors for sent vs received private messages
    const chipColor = isSender 
      ? (recipients.length > 1 ? "#8b5cf6" : "#f59e0b")  // Orange/Purple for sent
      : "#10b981";  // Green for received

    return (
      <Chip
        icon={<Lock sx={{ fontSize: 10 }} />}
        label={getPrivateLabel()}
        size="small"
        sx={{
          height: 18,
          fontSize: "0.65rem",
          backgroundColor: chipColor,
          color: "white",
          "& .MuiChip-icon": {
            fontSize: 10,
            marginLeft: "4px",
          },
          "& .MuiChip-label": {
            paddingLeft: "4px",
            paddingRight: "8px",
          },
        }}
      />
    );
  })()}

                        <Typography
  variant="caption"
  sx={{ 
    color: isOwnMessage ? "rgba(255,255,255,0.7)" : "#9ca3af", 
    fontSize: "0.7rem" 
  }}
>
  {formatMessageTime(message.timestamp)}
</Typography>
                      </Box>

                    <Paper
  elevation={0}
  sx={{
    p: 1.5,
    px: 2,
    backgroundColor: isOwnMessage ? "#3b82f6" : "#f3f4f6",
    color: isOwnMessage ? "white" : "#374151",
    borderRadius: isOwnMessage
      ? "18px 18px 4px 18px"
      : "18px 18px 18px 4px",
    maxWidth: "100%",
    wordBreak: "break-word",
    boxShadow: isOwnMessage 
      ? "0 1px 2px rgba(59, 130, 246, 0.2)" 
      : "0 1px 2px rgba(0, 0, 0, 0.05)",
    border: isPrivateMessage
      ? `2px solid ${
          (message.recipients || []).length > 1
            ? "#8b5cf6"
            : "#f59e0b"
        }`
      : "none",
                          cursor: isFileMessage ? "pointer" : "default",
                          transition: "all 0.2s ease",
                          "&:hover": isFileMessage
                            ? {
                                transform: "translateY(-1px)",
                                boxShadow: "0 4px 12px rgba(0,0,0,0.1)",
                              }
                            : {},
                        }}
                        onClick={
                          isFileMessage
                            ? () => {
                                let fileInfo = message.fileData;
                                if (!fileInfo) {
                                  fileInfo = extractFileInfoFromMessage(
                                    message.message
                                  );
                                }
                                if (fileInfo) {
                                  handleFileClick(fileInfo, message);
                                }
                              }
                            : undefined
                        }
                      >
                        {isFileMessage ? (
                          <Box>
                            {(() => {
                              let fileInfo = message.fileData;

                              if (!fileInfo) {
                                fileInfo = extractFileInfoFromMessage(
                                  message.message
                                );
                              }

                              if (!fileInfo) {
                                return (
                                  <Typography
                                    variant="body2"
                                    sx={{ lineHeight: 1.5 }}
                                  >
                                    {message.message || ""}
                                  </Typography>
                                );
                              }

                              return (
                                <Box>
                                  {fileInfo.type?.startsWith("image/") &&
                                    fileInfo.url &&
                                    !fileInfo.isFromPreviousSession && (
                                      <Box sx={{ mb: 2 }}>
                                        <img
                                          src={fileInfo.url}
                                          alt={fileInfo.name}
                                          style={{
                                            width: "100%",
                                            maxWidth: "240px",
                                            maxHeight: "180px",
                                            objectFit: "cover",
                                            borderRadius: "12px",
                                            display: "block",
                                          }}
                                          onError={(e) => {
                                            e.target.style.display = "none";
                                          }}
                                        />
                                      </Box>
                                    )}

                                  <Box
                                    sx={{
                                      display: "flex",
                                      alignItems: "center",
                                      gap: 2,
                                    }}
                                  >
                                    <Box
                                      sx={{
                                        display: "flex",
                                        alignItems: "center",
                                        justifyContent: "center",
                                        width: 48,
                                        height: 48,
                                        borderRadius: 2,
                                        backgroundColor: isOwnMessage
                                          ? "rgba(255, 255, 255, 0.2)"
                                          : "#e5e7eb",
                                      }}
                                    >
                                      {fileInfo.isFromPreviousSession ? (
                                        <InsertDriveFile
                                          sx={{
                                            color: isOwnMessage
                                              ? "rgba(255, 255, 255, 0.7)"
                                              : "#6b7280",
                                          }}
                                        />
                                      ) : (
                                        getFileIcon(fileInfo.type)
                                      )}
                                    </Box>

                                    <Box sx={{ flex: 1, minWidth: 0 }}>
                                      <Typography
                                        variant="body2"
                                        sx={{
                                          lineHeight: 1.4,
                                          fontWeight: 600,
                                          mb: 0.5,
                                          overflow: "hidden",
                                          textOverflow: "ellipsis",
                                          whiteSpace: "nowrap",
                                        }}
                                      >
                                        {fileInfo.name}
                                      </Typography>

                                      <Box
                                        sx={{
                                          display: "flex",
                                          alignItems: "center",
                                          gap: 1,
                                          mb: 0.5,
                                        }}
                                      >
                                        <Typography
                                          variant="caption"
                                          sx={{
                                            color: isOwnMessage
                                              ? "rgba(255, 255, 255, 0.8)"
                                              : "#6b7280",
                                            fontSize: "0.75rem",
                                          }}
                                        >
                                          {formatFileSize(fileInfo.size)}
                                        </Typography>

                                        <Chip
                                          label={
                                            getFileExtension(fileInfo.name) ||
                                            "FILE"
                                          }
                                          size="small"
                                          sx={{
                                            height: "20px",
                                            fontSize: "0.6rem",
                                            backgroundColor: isOwnMessage
                                              ? "rgba(255, 255, 255, 0.2)"
                                              : "#d1d5db",
                                            color: isOwnMessage
                                              ? "white"
                                              : "#374151",
                                            "& .MuiChip-label": { px: 1 },
                                          }}
                                        />
                                      </Box>

                                      <Typography
                                        variant="caption"
                                        sx={{
                                          color: fileInfo.isFromPreviousSession
                                            ? isOwnMessage
                                              ? "rgba(255, 235, 59, 0.8)"
                                              : "#f59e0b"
                                            : isOwnMessage
                                            ? "rgba(255, 255, 255, 0.7)"
                                            : "#6b7280",
                                          fontSize: "0.7rem",
                                          fontStyle: "italic",
                                          display: "block",
                                        }}
                                      >
                                        {fileInfo.isFromPreviousSession
                                          ? "From previous session"
                                          : "Click to download"}
                                      </Typography>
                                    </Box>

                                    <GetApp
                                      sx={{
                                        fontSize: 20,
                                        color: isOwnMessage
                                          ? "rgba(255, 255, 255, 0.7)"
                                          : "#6b7280",
                                      }}
                                    />
                                  </Box>
                                </Box>
                              );
                            })()}
                          </Box>
                        ) : (
                          <Typography variant="body2" sx={{ lineHeight: 1.5 }}>
                            {message.message || ""}
                          </Typography>
                        )}
                      </Paper>
                    </Box>
                  </Box>
                );
              })}

              {uploadingFiles.map((upload, index) => (
                <Box
                  key={upload.id || index}
                  sx={{
                    mb: 3,
                    display: "flex",
                    flexDirection: "row-reverse",
                    alignItems: "flex-start",
                    gap: 2,
                  }}
                >
                  <Avatar
                    sx={{
                      width: 40,
                      height: 40,
                      fontSize: "0.875rem",
                      backgroundColor: "#3b82f6",
                      fontWeight: 600,
                      flexShrink: 0,
                    }}
                  >
                    {getInitials(
                      currentUser?.name || currentUser?.full_name || "You"
                    )}
                  </Avatar>

                  <Paper
                    elevation={0}
                    sx={{
                      p: 2.5,
                      backgroundColor: "rgba(59, 130, 246, 0.1)",
                      borderRadius: "20px 20px 4px 20px",
                      maxWidth: "calc(100% - 56px)",
                      border: "1px solid rgba(59, 130, 246, 0.2)",
                    }}
                  >
                    <Box
                      sx={{
                        display: "flex",
                        alignItems: "center",
                        gap: 2,
                        mb: 2,
                      }}
                    >
                      <Box
                        sx={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "center",
                          width: 40,
                          height: 40,
                          borderRadius: 2,
                          backgroundColor: "#e5e7eb",
                        }}
                      >
                        {getFileIcon(upload.file.type)}
                      </Box>

                      <Box sx={{ flex: 1, minWidth: 0 }}>
                        <Typography
                          variant="body2"
                          sx={{
                            fontWeight: 600,
                            mb: 0.5,
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            whiteSpace: "nowrap",
                            color: "#374151",
                          }}
                        >
                          {upload.file.name}
                        </Typography>
                        <Typography variant="caption" sx={{ color: "#6b7280" }}>
                          {formatFileSize(upload.file.size)}
                        </Typography>
                      </Box>
                    </Box>

                    <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
                      <Box
                        sx={{
                          flex: 1,
                          height: 8,
                          backgroundColor: "#e5e7eb",
                          borderRadius: 1,
                          overflow: "hidden",
                        }}
                      >
                        <Box
                          sx={{
                            height: "100%",
                            backgroundColor: "#3b82f6",
                            width: `${upload.progress}%`,
                            transition: "width 0.3s ease",
                            borderRadius: 1,
                          }}
                        />
                      </Box>
                      <Typography
                        variant="caption"
                        sx={{
                          fontSize: "0.75rem",
                          fontWeight: 600,
                          minWidth: "40px",
                          textAlign: "right",
                          color: "#374151",
                        }}
                      >
                        {upload.progress}%
                      </Typography>
                    </Box>

                    <Typography
                      variant="caption"
                      sx={{
                        color: "#6b7280",
                        fontSize: "0.75rem",
                        mt: 1,
                        display: "block",
                      }}
                    >
                      Uploading...
                    </Typography>
                  </Paper>
                </Box>
              ))}

              {typingUsers.size > 0 && (
                <Box
                  sx={{
                    display: "flex",
                    alignItems: "center",
                    gap: 2,
                    mt: 2,
                    mb: 3,
                  }}
                >
                  <Avatar
                    sx={{
                      width: 32,
                      height: 32,
                      backgroundColor: "#e5e7eb",
                      fontSize: "0.75rem",
                    }}
                  >
                    <Typography sx={{ color: "#6b7280" }}>...</Typography>
                  </Avatar>
                  <Typography variant="caption" sx={{ color: "#6b7280" }}>
                    {Array.from(typingUsers).join(", ")}{" "}
                    {typingUsers.size === 1 ? "is" : "are"} typing
                    <Box
                      component="span"
                      sx={{ display: "inline-flex", gap: 0.25, ml: 0.5 }}
                    >
                      {[0, 1, 2].map((i) => (
                        <Box
                          key={i}
                          sx={{
                            width: 4,
                            height: 4,
                            borderRadius: "50%",
                            backgroundColor: "#6b7280",
                            animation: "typing 1.4s infinite",
                            animationDelay: `${i * 0.2}s`,
                            "@keyframes typing": {
                              "0%, 60%": { transform: "translateY(0)" },
                              "30%": { transform: "translateY(-6px)" },
                            },
                          }}
                        />
                      ))}
                    </Box>
                  </Typography>
                </Box>
              )}

              <div ref={messagesEndRef} />
            </Box>
          )}
        </Box>

        <Box
          sx={{
            p: 1,
            backgroundColor: "#ffffff",
            borderTop: "1px solid #f3f4f6",
            maxHeight: "50%",
            overflowY: "auto",
            "&::-webkit-scrollbar": { width: "6px" },
            "&::-webkit-scrollbar-track": { background: "#f3f4f6" },
            "&::-webkit-scrollbar-thumb": {
              background: "#d1d5db",
              borderRadius: "3px",
            },
          }}
        >
          {/* Recipient Selector for Non-Host */}
          {!isHost && privateChatEnabled && (
            <Box
              sx={{ display: "flex", alignItems: "center", gap: 1, mb: 1, px: 1 }}
            >
              <Typography
                variant="caption"
                sx={{ color: "#6b7280", fontWeight: 500 }}
              >
                Send to:
              </Typography>
              <Box sx={{ display: "flex", gap: 0.5 }}>
                <Chip
                  icon={<Public sx={{ fontSize: 14 }} />}
                  label="Everyone"
                  size="small"
                  clickable
                  onClick={(e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    setMessageRecipient("everyone");
                  }}
                  sx={{
                    backgroundColor:
                      messageRecipient === "everyone" ? "#10b981" : "#e5e7eb",
                    color: messageRecipient === "everyone" ? "white" : "#6b7280",
                    fontSize: "0.75rem",
                    height: "26px",
                    fontWeight: 600,
                    transition: "all 0.2s ease",
                    cursor: "pointer",
                    "&:hover": {
                      backgroundColor:
                        messageRecipient === "everyone" ? "#059669" : "#d1d5db",
                    },
                  }}
                />
                <Chip
                  icon={<Lock sx={{ fontSize: 14 }} />}
                  label="Host Only"
                  size="small"
                  clickable
                  onClick={(e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    setMessageRecipient("host");
                  }}
                  sx={{
                    backgroundColor:
                      messageRecipient === "host" ? "#f59e0b" : "#e5e7eb",
                    color: messageRecipient === "host" ? "white" : "#6b7280",
                    fontSize: "0.75rem",
                    height: "26px",
                    fontWeight: 600,
                    transition: "all 0.2s ease",
                    cursor: "pointer",
                    "&:hover": {
                      backgroundColor:
                        messageRecipient === "host" ? "#d97706" : "#d1d5db",
                    },
                  }}
                />
              </Box>
            </Box>
          )}

          {/* Recipient Selector for Host */}
          {isHost && (
            <Box sx={{ px: 1, mb: 2 }}>
              <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 1 }}>
                <Typography
                  variant="caption"
                  sx={{ color: "#6b7280", fontWeight: 500 }}
                >
                  Send to:
                </Typography>
                <Box sx={{ display: "flex", gap: 0.5, flex: 1 }}>
                  {/* Everyone chip */}
                  <Chip
                    icon={<Public sx={{ fontSize: 14 }} />}
                    label="Everyone"
                    size="small"
                    clickable
                    onClick={handleEveryone}
                    sx={{
                      backgroundColor:
                        messageRecipient === "everyone" ? "#10b981" : "#e5e7eb",
                      color:
                        messageRecipient === "everyone" ? "white" : "#6b7280",
                      fontSize: "0.75rem",
                      height: "26px",
                      fontWeight: 600,
                      transition: "all 0.2s ease",
                      cursor: "pointer",
                      "&:hover": {
                        backgroundColor:
                          messageRecipient === "everyone"
                            ? "#059669"
                            : "#d1d5db",
                      },
                    }}
                  />

                  {/* Select chip */}
                  <Chip
                    icon={
                      showRecipientSelector ? (
                        <ExpandLess sx={{ fontSize: 14 }} />
                      ) : (
                        <People sx={{ fontSize: 14 }} />
                      )
                    }
                    label={
                      Array.isArray(messageRecipient) &&
                      messageRecipient.length > 0
                        ? `${messageRecipient.length} selected`
                        : "Select"
                    }
                    size="small"
                    clickable
                    onClick={handleRecipientToggle}
                    sx={{
                      backgroundColor:
                        Array.isArray(messageRecipient) &&
                        messageRecipient.length > 0
                          ? "#8b5cf6"
                          : "#e5e7eb",
                      color:
                        Array.isArray(messageRecipient) &&
                        messageRecipient.length > 0
                          ? "white"
                          : "#6b7280",
                      fontSize: "0.75rem",
                      height: "26px",
                      fontWeight: 600,
                      transition: "all 0.2s ease",
                      cursor: "pointer",
                      "&:hover": {
                        backgroundColor:
                          Array.isArray(messageRecipient) &&
                          messageRecipient.length > 0
                            ? "#7c3aed"
                            : "#d1d5db",
                      },
                    }}
                  />
                </Box>
              </Box>

              {/* Integrated recipient selector dropdown */}
              <Collapse in={showRecipientSelector} timeout="auto" unmountOnExit>
                <Paper
                  elevation={0}
                  sx={{
                    backgroundColor: "#f9fafb",
                    border: "1px solid #e5e7eb",
                    borderRadius: 2,
                    p: 2,
                    mb: 2,
                  }}
                >
                  {/* Search field */}
                  <TextField
                    fullWidth
                    size="small"
                    placeholder="Search participants..."
                    value={recipientSearchTerm}
                    onChange={(e) => setRecipientSearchTerm(e.target.value)}
                    InputProps={{
                      startAdornment: (
                        <InputAdornment position="start">
                          <Search sx={{ fontSize: 18, color: "#6b7280" }} />
                        </InputAdornment>
                      ),
                    }}
                    sx={{
                      mb: 2,
                      "& .MuiOutlinedInput-root": {
                        borderRadius: 2,
                        backgroundColor: "#ffffff",
                        fontSize: "0.875rem",
                      },
                    }}
                  />

                  {/* Participants list */}
                  <Box
                    sx={{
                      maxHeight: "200px",
                      overflowY: "auto",
                      "&::-webkit-scrollbar": { width: "6px" },
                      "&::-webkit-scrollbar-track": { background: "#f3f4f6" },
                      "&::-webkit-scrollbar-thumb": {
                        background: "#d1d5db",
                        borderRadius: "3px",
                      },
                    }}
                  >
                    {filteredParticipants.length === 0 ? (
                      <Typography
                        variant="body2"
                        sx={{ color: "#9ca3af", textAlign: "center", py: 2 }}
                      >
                        No participants found
                      </Typography>
                    ) : (
                      <List dense sx={{ p: 0 }}>
                        {filteredParticipants.map((participant) => {
                          const participantId =
                            participant.user_id || participant.id;
                          const isSelected =
                            selectedParticipants.includes(participantId);

                          return (
                            <ListItem
                              key={participantId}
                              button
                              onClick={() =>
                                handleSelectParticipant(participantId)
                              }
                              sx={{
                                borderRadius: 1.5,
                                mb: 0.5,
                                backgroundColor: isSelected
                                  ? "#ede9fe"
                                  : "transparent",
                                "&:hover": {
                                  backgroundColor: isSelected
                                    ? "#ddd6fe"
                                    : "#f3f4f6",
                                },
                              }}
                            >
                              <Box
                                sx={{
                                  display: "flex",
                                  alignItems: "center",
                                  gap: 2,
                                  width: "100%",
                                }}
                              >
                                <Avatar
                                  sx={{
                                    width: 32,
                                    height: 32,
                                    fontSize: "0.75rem",
                                    backgroundColor: getParticipantColor(
                                      participantId
                                    ),
                                    fontWeight: 600,
                                    flexShrink: 0,
                                  }}
                                >
                                  {getInitials(
                                    participant.displayName ||
                                      participant.name ||
                                      participant.full_name
                                  )}
                                </Avatar>
                                <Box sx={{ flex: 1, minWidth: 0 }}>
                                  <Typography
                                    variant="body2"
                                    sx={{
                                      fontWeight: 500,
                                      color: "#374151",
                                      overflow: "hidden",
                                      textOverflow: "ellipsis",
                                      whiteSpace: "nowrap",
                                    }}
                                  >
                                    {participant.displayName ||
                                      participant.name ||
                                      participant.full_name}
                                  </Typography>
                                </Box>
                                {isSelected && (
                                  <CheckCircle
                                    sx={{
                                      fontSize: 20,
                                      color: "#8b5cf6",
                                      flexShrink: 0,
                                    }}
                                  />
                                )}
                              </Box>
                            </ListItem>
                          );
                        })}
                      </List>
                    )}
                  </Box>

                  {/* Action buttons */}
                  <Divider sx={{ my: 2 }} />
                  <Box sx={{ display: "flex", gap: 1 }}>
                    <Button
                      fullWidth
                      variant="outlined"
                      size="small"
                      onClick={handleCancelSelection}
                      sx={{
                        textTransform: "none",
                        borderColor: "#e5e7eb",
                        color: "#6b7280",
                        fontSize: "0.875rem",
                        "&:hover": {
                          borderColor: "#d1d5db",
                          backgroundColor: "#f9fafb",
                        },
                      }}
                    >
                      Cancel
                    </Button>
                    <Button
                      fullWidth
                      variant="contained"
                      size="small"
                      onClick={handleApplySelection}
                      disabled={selectedParticipants.length === 0}
                      sx={{
                        textTransform: "none",
                        backgroundColor: "#8b5cf6",
                        fontSize: "0.875rem",
                        "&:hover": {
                          backgroundColor: "#7c3aed",
                        },
                        "&:disabled": {
                          backgroundColor: "#e5e7eb",
                          color: "#9ca3af",
                        },
                      }}
                    >
                      Apply Selection
                    </Button>
                  </Box>
                </Paper>
              </Collapse>
            </Box>
          )}

          <Box
            sx={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              mb: 1,
              px: 1,
            }}
          >
            <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
              {messageRecipient !== "everyone" && (
                <Chip
                  icon={
                    messageRecipient === "host" ? (
                      <Lock sx={{ fontSize: 14 }} />
                    ) : (
                      <People sx={{ fontSize: 14 }} />
                    )
                  }
                  label={
                    messageRecipient === "host"
                      ? "Private (Host Only)"
                      : `Private (${
                          Array.isArray(messageRecipient)
                            ? messageRecipient.length
                            : 0
                        } selected)`
                  }
                  size="small"
                  sx={{
                    backgroundColor:
                      messageRecipient === "host" ? "#f59e0b" : "#8b5cf6",
                    color: "white",
                    fontSize: "0.75rem",
                    height: "24px",
                    fontWeight: 600,
                  }}
                />
              )}
              <Typography variant="caption" sx={{ color: "#6b7280" }}>
                {getSyncStatusIcon()}
                Connected
              </Typography>
            </Box>
          </Box>

          <Box
            sx={{
              display: "flex",
              alignItems: "flex-end",
              gap: 2,
              backgroundColor: "#f9fafb",
              borderRadius: 3,
              border: "1px solid #e5e7eb",
              p: 1,
              "&:focus-within": {
                borderColor: "#3b82f6",
                boxShadow: "0 0 0 3px rgba(59, 130, 246, 0.1)",
              },
              transition: "all 0.2s ease",
            }}
          >
            <IconButton
              size="small"
              onClick={handleEmojiToggle}
              sx={{
                color: "#6b7280",
                "&:hover": {
                  color: "#3b82f6",
                  backgroundColor: "rgba(59, 130, 246, 0.1)",
                },
              }}
            >
              <EmojiEmotions sx={{ fontSize: 20 }} />
            </IconButton>

            <IconButton
              size="small"
              onClick={handleFileUpload}
              disabled={!chatPermissions.canUploadFiles}
              sx={{
                color: "#6b7280",
                "&:hover": {
                  color: "#3b82f6",
                  backgroundColor: "rgba(59, 130, 246, 0.1)",
                },
                "&:disabled": { color: "#d1d5db" },
              }}
            >
              <AttachFile sx={{ fontSize: 20 }} />
            </IconButton>

            <TextField
              ref={inputRef}
              fullWidth
              multiline
              maxRows={4}
              value={newMessage}
              onChange={(e) => setNewMessage(e.target.value)}
              onKeyPress={handleKeyPress}
              placeholder="Type your message..."
              disabled={!chatPermissions.canSendMessages || !isChatInitialized}
              variant="standard"
              InputProps={{
                disableUnderline: true,
                sx: {
                  fontSize: "0.875rem",
                  lineHeight: 1.5,
                  color: "#374151",
                  "& input, & textarea": {
                    padding: "5px 0",
                  },
                  "&::placeholder": {
                    color: "#9ca3af",
                  },
                },
              }}
            />

            <IconButton
              onClick={handleSendMessage}
              disabled={
                !newMessage.trim() ||
                !chatPermissions.canSendMessages ||
                !isChatInitialized
              }
              sx={{
                backgroundColor: newMessage.trim() ? "#3b82f6" : "#e5e7eb",
                color: newMessage.trim() ? "white" : "#9ca3af",
                width: 40,
                height: 40,
                borderRadius: 2,
                "&:hover": {
                  backgroundColor: newMessage.trim() ? "#2563eb" : "#e5e7eb",
                  transform: newMessage.trim() ? "scale(1.05)" : "none",
                },
                "&:disabled": {
                  backgroundColor: "#e5e7eb",
                  color: "#9ca3af",
                },
                transition: "all 0.2s ease",
              }}
            >
              {isChatInitialized ? (
                <Send sx={{ fontSize: 18 }} />
              ) : (
                <CircularProgress size={16} />
              )}
            </IconButton>
          </Box>
        </Box>

        <Popover
          open={showEmojiPicker}
          anchorEl={emojiAnchorEl}
          onClose={() => {
            setShowEmojiPicker(false);
            setEmojiAnchorEl(null);
          }}
          anchorOrigin={{ vertical: "top", horizontal: "center" }}
          transformOrigin={{ vertical: "bottom", horizontal: "center" }}
          PaperProps={{
            sx: {
              backgroundColor: "#ffffff",
              border: "1px solid #e5e7eb",
              borderRadius: 3,
              width: "320px",
              maxHeight: "400px",
              overflow: "hidden",
              boxShadow:
                "0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04)",
            },
          }}
        >
          <Box sx={{ p: 2 }}>
            <Box
              sx={{
                display: "flex",
                gap: 0.5,
                mb: 2,
                overflowX: "auto",
                "&::-webkit-scrollbar": { display: "none" },
              }}
            >
              {Object.keys(EMOJI_CATEGORIES).map((category) => (
                <Chip
                  key={category}
                  label={category.split(" ")[0]}
                  size="small"
                  onClick={() => setSelectedEmojiCategory(category)}
                  sx={{
                    backgroundColor:
                      selectedEmojiCategory === category
                        ? "#3b82f6"
                        : "#f3f4f6",
                    color:
                      selectedEmojiCategory === category ? "white" : "#6b7280",
                    fontSize: "0.75rem",
                    height: "28px",
                    fontWeight: 600,
                    cursor: "pointer",
                    "&:hover": {
                      backgroundColor:
                        selectedEmojiCategory === category
                          ? "#2563eb"
                          : "#e5e7eb",
                    },
                  }}
                />
              ))}
            </Box>

            <Box
              sx={{
                maxHeight: "250px",
                overflowY: "auto",
                "&::-webkit-scrollbar": { width: "6px" },
                "&::-webkit-scrollbar-track": { background: "#f3f4f6" },
                "&::-webkit-scrollbar-thumb": {
                  background: "#d1d5db",
                  borderRadius: "3px",
                },
              }}
            >
              <Grid container spacing={0.5}>
                {EMOJI_CATEGORIES[selectedEmojiCategory]?.map(
                  (emoji, index) => (
                    <Grid item key={index}>
                      <IconButton
                        size="small"
                        onClick={() => handleEmojiSelect(emoji)}
                        sx={{
                          width: 32,
                          height: 32,
                          fontSize: "1.2rem",
                          "&:hover": {
                            backgroundColor: "#f3f4f6",
                            transform: "scale(1.2)",
                          },
                          transition: "all 0.1s ease",
                        }}
                      >
                        {emoji}
                      </IconButton>
                    </Grid>
                  )
                )}
              </Grid>
            </Box>
          </Box>
        </Popover>
      </Box>
    </>
  );
};

export default ChatPanel;