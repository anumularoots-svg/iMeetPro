import { useState, useEffect, useCallback } from 'react';
import { useLiveKit } from './useLiveKit';
import { useAuth } from './useAuth';

export const useInteractions = (meetingId) => {
  const [raisedHands, setRaisedHands] = useState(new Map());
  const [polls, setPolls] = useState([]);
  const [activePoll, setActivePoll] = useState(null);
  const [qnaQuestions, setQnaQuestions] = useState([]);
  const [annotations, setAnnotations] = useState([]);
  const [reactions, setReactions] = useState([]);
  const [notifications, setNotifications] = useState([]);
  
  const { socket, raiseHand, addEventListener, removeEventListener } = useLiveKit();
  const { user } = useAuth();

  useEffect(() => {
    if (socket && meetingId) {
      setupInteractionListeners();
    }

    return () => {
      cleanupInteractionListeners();
    };
  }, [socket, meetingId]);

  const setupInteractionListeners = () => {
    addEventListener('hand-raised', handleHandRaised);
    addEventListener('hand-lowered', handleHandLowered);
    addEventListener('poll-created', handlePollCreated);
    addEventListener('poll-updated', handlePollUpdated);
    addEventListener('poll-ended', handlePollEnded);
    addEventListener('question-asked', handleQuestionAsked);
    addEventListener('question-answered', handleQuestionAnswered);
    addEventListener('annotation-added', handleAnnotationAdded);
    addEventListener('annotation-removed', handleAnnotationRemoved);
    addEventListener('interaction-notification', handleInteractionNotification);
  };

  const cleanupInteractionListeners = () => {
    removeEventListener('hand-raised', handleHandRaised);
    removeEventListener('hand-lowered', handleHandLowered);
    removeEventListener('poll-created', handlePollCreated);
    removeEventListener('poll-updated', handlePollUpdated);
    removeEventListener('poll-ended', handlePollEnded);
    removeEventListener('question-asked', handleQuestionAsked);
    removeEventListener('question-answered', handleQuestionAnswered);
    removeEventListener('annotation-added', handleAnnotationAdded);
    removeEventListener('annotation-removed', handleAnnotationRemoved);
    removeEventListener('interaction-notification', handleInteractionNotification);
  };

  // Hand raising functionality
  const handleHandRaised = useCallback((data) => {
    const { participantId, participantName, timestamp } = data;
    setRaisedHands(prev => {
      const newMap = new Map(prev);
      newMap.set(participantId, {
        participantId,
        participantName,
        timestamp,
        acknowledged: false
      });
      return newMap;
    });
    
    playNotificationSound('hand-raise');
  }, []);

  const handleHandLowered = useCallback((data) => {
    const { participantId } = data;
    setRaisedHands(prev => {
      const newMap = new Map(prev);
      newMap.delete(participantId);
      return newMap;
    });
  }, []);

  const toggleHandRaise = useCallback(() => {
    const isCurrentlyRaised = raisedHands.has(user?.id);
    
    if (socket && meetingId) {
      raiseHand(meetingId, !isCurrentlyRaised);
    }
  }, [socket, meetingId, raisedHands, user?.id, raiseHand]);

  const acknowledgeHand = useCallback((participantId) => {
    setRaisedHands(prev => {
      const newMap = new Map(prev);
      const handData = newMap.get(participantId);
      if (handData) {
        newMap.set(participantId, { ...handData, acknowledged: true });
      }
      return newMap;
    });

    if (socket && meetingId) {
      socket.emit('hand-acknowledged', { meetingId, participantId });
    }
  }, [socket, meetingId]);

  const clearRaisedHand = useCallback((participantId) => {
    setRaisedHands(prev => {
      const newMap = new Map(prev);
      newMap.delete(participantId);
      return newMap;
    });

    if (socket && meetingId) {
      socket.emit('hand-cleared', { meetingId, participantId });
    }
  }, [socket, meetingId]);

  // Polling functionality
  const handlePollCreated = useCallback((pollData) => {
    setPolls(prev => [...prev, pollData]);
    setActivePoll(pollData);
    playNotificationSound('poll');
  }, []);

  const handlePollUpdated = useCallback((pollData) => {
    setPolls(prev => prev.map(p => p.id === pollData.id ? pollData : p));
    if (activePoll?.id === pollData.id) {
      setActivePoll(pollData);
    }
  }, [activePoll]);

  const handlePollEnded = useCallback((pollData) => {
    setPolls(prev => prev.map(p => p.id === pollData.id ? { ...p, ended: true } : p));
    if (activePoll?.id === pollData.id) {
      setActivePoll(null);
    }
  }, [activePoll]);

  const createPoll = useCallback((pollData) => {
    if (socket && meetingId) {
      const poll = {
        id: Date.now().toString(),
        meetingId,
        createdBy: user?.id,
        createdByName: user?.full_name,
        question: pollData.question,
        options: pollData.options,
        allowMultiple: pollData.allowMultiple || false,
        anonymous: pollData.anonymous || false,
        responses: [],
        createdAt: new Date().toISOString(),
        endTime: pollData.duration ? new Date(Date.now() + pollData.duration * 1000).toISOString() : null
      };

      socket.emit('create-poll', { meetingId, poll });
    }
  }, [socket, meetingId, user]);

  const votePoll = useCallback((pollId, optionIds) => {
    if (socket && meetingId) {
      const vote = {
        pollId,
        participantId: user?.id,
        participantName: user?.full_name,
        optionIds: Array.isArray(optionIds) ? optionIds : [optionIds],
        timestamp: new Date().toISOString()
      };

      socket.emit('vote-poll', { meetingId, vote });
    }
  }, [socket, meetingId, user]);

  const endPoll = useCallback((pollId) => {
    if (socket && meetingId) {
      socket.emit('end-poll', { meetingId, pollId });
    }
  }, [socket, meetingId]);

  // Q&A functionality
  const handleQuestionAsked = useCallback((questionData) => {
    setQnaQuestions(prev => [...prev, questionData]);
    playNotificationSound('question');
  }, []);

  const handleQuestionAnswered = useCallback((answerData) => {
    setQnaQuestions(prev => 
      prev.map(q => 
        q.id === answerData.questionId 
          ? { ...q, answer: answerData.answer, answeredAt: answerData.timestamp }
          : q
      )
    );
  }, []);

  const askQuestion = useCallback((question) => {
    if (socket && meetingId) {
      const questionData = {
        id: Date.now().toString(),
        meetingId,
        question,
        askedBy: user?.id,
        askedByName: user?.full_name,
        timestamp: new Date().toISOString(),
        upvotes: 0,
        answered: false
      };

      socket.emit('ask-question', { meetingId, question: questionData });
    }
  }, [socket, meetingId, user]);

  const answerQuestion = useCallback((questionId, answer) => {
    if (socket && meetingId) {
      const answerData = {
        questionId,
        answer,
        answeredBy: user?.id,
        answeredByName: user?.full_name,
        timestamp: new Date().toISOString()
      };

      socket.emit('answer-question', { meetingId, answer: answerData });
    }
  }, [socket, meetingId, user]);

  const upvoteQuestion = useCallback((questionId) => {
    if (socket && meetingId) {
      socket.emit('upvote-question', { meetingId, questionId, userId: user?.id });
    }
  }, [socket, meetingId, user]);

  // Annotation functionality
  const handleAnnotationAdded = useCallback((annotationData) => {
    setAnnotations(prev => [...prev, annotationData]);
  }, []);

  const handleAnnotationRemoved = useCallback((annotationData) => {
    setAnnotations(prev => prev.filter(a => a.id !== annotationData.id));
  }, []);

  const addAnnotation = useCallback((annotationData) => {
    if (socket && meetingId) {
      const annotation = {
        id: Date.now().toString(),
        meetingId,
        type: annotationData.type, // 'highlight', 'arrow', 'text', 'circle'
        position: annotationData.position,
        data: annotationData.data,
        createdBy: user?.id,
        createdByName: user?.full_name,
        timestamp: new Date().toISOString(),
        duration: annotationData.duration || 5000 // 5 seconds default
      };

      socket.emit('add-annotation', { meetingId, annotation });
    }
  }, [socket, meetingId, user]);

  const removeAnnotation = useCallback((annotationId) => {
    if (socket && meetingId) {
      socket.emit('remove-annotation', { meetingId, annotationId });
    }
  }, [socket, meetingId]);

  // Notification handling
  const handleInteractionNotification = useCallback((notification) => {
    setNotifications(prev => [...prev.slice(-9), notification]); // Keep last 10 notifications
    
    // Auto-remove notification after delay
    setTimeout(() => {
      setNotifications(prev => prev.filter(n => n.id !== notification.id));
    }, notification.duration || 5000);
  }, []);

  const dismissNotification = useCallback((notificationId) => {
    setNotifications(prev => prev.filter(n => n.id !== notificationId));
  }, []);

  // Utility functions
  const playNotificationSound = (type) => {
    try {
      const soundMap = {
        'hand-raise': '/sounds/hand-raise.mp3',
        'poll': '/sounds/notification.mp3',
        'question': '/sounds/notification.mp3',
        'general': '/sounds/notification.mp3'
      };
      
      const audio = new Audio(soundMap[type] || soundMap.general);
      audio.volume = 0.3;
      audio.play().catch(err => console.log('Could not play sound:', err));
    } catch (error) {
      console.log('Error playing notification sound:', error);
    }
  };

  const getRaisedHandsCount = () => {
    return raisedHands.size;
  };

  const getRaisedHandsList = () => {
    return Array.from(raisedHands.values())
      .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  };

  const getActivePollResults = () => {
    if (!activePoll) return null;
    
    const results = activePoll.options.map(option => ({
      ...option,
      votes: activePoll.responses.filter(r => r.optionIds.includes(option.id)).length
    }));
    
    const totalVotes = activePoll.responses.length;
    
    return {
      ...activePoll,
      results,
      totalVotes,
      participationRate: totalVotes > 0 ? ((totalVotes / participantCount) * 100).toFixed(1) : 0
    };
  };

  const getPendingQuestions = () => {
    return qnaQuestions
      .filter(q => !q.answered)
      .sort((a, b) => b.upvotes - a.upvotes || new Date(a.timestamp) - new Date(b.timestamp));
  };

  const getAnsweredQuestions = () => {
    return qnaQuestions
      .filter(q => q.answered)
      .sort((a, b) => new Date(b.answeredAt) - new Date(a.answeredAt));
  };

  return {
    // Hand raising
    raisedHands,
    toggleHandRaise,
    acknowledgeHand,
    clearRaisedHand,
    getRaisedHandsCount,
    getRaisedHandsList,
    
    // Polling
    polls,
    activePoll,
    createPoll,
    votePoll,
    endPoll,
    getActivePollResults,
    
    // Q&A
    qnaQuestions,
    askQuestion,
    answerQuestion,
    upvoteQuestion,
    getPendingQuestions,
    getAnsweredQuestions,
    
    // Annotations
    annotations,
    addAnnotation,
    removeAnnotation,
    
    // Notifications
    notifications,
    dismissNotification,
    
    // Utilities
    playNotificationSound
  };
};