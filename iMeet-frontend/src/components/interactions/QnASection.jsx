import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  TextField,
  Button,
  IconButton,
  Avatar,
  Chip,
  Paper,
  List,
  ListItem,
  ListItemAvatar,
  ListItemText,
  ListItemSecondaryAction,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Tabs,
  Tab,
  Badge,
  Tooltip,
  Menu,
  MenuItem,
  Divider,
  Fade,
  Zoom
} from '@mui/material';
import {
  QuestionAnswer as QnaIcon,
  Send as SendIcon,
  ThumbUp as LikeIcon,
  ThumbDown as DislikeIcon,
  Reply as ReplyIcon,
  MoreVert as MoreIcon,
  Star as FeaturedIcon,
  CheckCircle as AnsweredIcon,
  Schedule as PendingIcon,
  Sort as SortIcon,
  Search as SearchIcon,
  Close as CloseIcon
} from '@mui/icons-material';
import { formatDistanceToNow } from 'date-fns';

const QnASection = ({ 
  questions = [], 
  onSubmitQuestion, 
  onAnswerQuestion,
  onLikeQuestion,
  onReplyToQuestion,
  onDeleteQuestion,
  isHost = false,
  currentUserId 
}) => {
  const [activeTab, setActiveTab] = useState(0);
  const [newQuestion, setNewQuestion] = useState('');
  const [answerDialog, setAnswerDialog] = useState({ open: false, question: null });
  const [answerText, setAnswerText] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [sortBy, setSortBy] = useState('recent'); // recent, popular, answered
  const [sortMenuAnchor, setSortMenuAnchor] = useState(null);

  const handleSubmitQuestion = () => {
    if (newQuestion.trim()) {
      onSubmitQuestion?.({
        text: newQuestion.trim(),
        timestamp: new Date().toISOString(),
        userId: currentUserId,
        status: 'pending'
      });
      setNewQuestion('');
    }
  };

  const handleAnswerQuestion = () => {
    if (answerText.trim() && answerDialog.question) {
      onAnswerQuestion?.(answerDialog.question.id, answerText.trim());
      setAnswerDialog({ open: false, question: null });
      setAnswerText('');
    }
  };

  const filterAndSortQuestions = (questions, status = null) => {
    let filtered = questions;
    
    // Filter by status
    if (status) {
      filtered = filtered.filter(q => q.status === status);
    }
    
    // Filter by search query
    if (searchQuery) {
      filtered = filtered.filter(q => 
        q.text?.toLowerCase().includes(searchQuery.toLowerCase()) ||
        q.user?.full_name?.toLowerCase().includes(searchQuery.toLowerCase())
      );
    }
    
    // Sort questions
    switch (sortBy) {
      case 'popular':
        return filtered.sort((a, b) => (b.likes || 0) - (a.likes || 0));
      case 'answered':
        return filtered.sort((a, b) => {
          if (a.status === 'answered' && b.status !== 'answered') return -1;
          if (a.status !== 'answered' && b.status === 'answered') return 1;
          return new Date(b.timestamp) - new Date(a.timestamp);
        });
      default: // recent
        return filtered.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    }
  };

  const getStatusIcon = (status) => {
    switch (status) {
      case 'answered':
        return <AnsweredIcon color="success" />;
      case 'featured':
        return <FeaturedIcon color="warning" />;
      default:
        return <PendingIcon color="action" />;
    }
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'answered':
        return 'success';
      case 'featured':
        return 'warning';
      default:
        return 'default';
    }
  };

  const QuestionCard = ({ question }) => {
    const [expanded, setExpanded] = useState(false);
    const [menuAnchor, setMenuAnchor] = useState(null);
    
    const hasUserLiked = question.likedBy?.includes(currentUserId);
    const canModerate = isHost || question.userId === currentUserId;

    return (
      <Fade in={true} timeout={300}>
        <Card 
          elevation={2} 
          sx={{ 
            mb: 2,
            border: question.status === 'featured' ? '2px solid #ff9800' : 'none',
            background: question.status === 'answered' 
              ? 'linear-gradient(135deg, #e8f5e8 0%, #f1f8e9 100%)'
              : question.status === 'featured'
              ? 'linear-gradient(135deg, #fff3e0 0%, #fef7ed 100%)'
              : 'white'
          }}
        >
          <CardContent>
            <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 2 }}>
              <Avatar 
                src={question.user?.profile_picture}
                sx={{ bgcolor: 'primary.main' }}
              >
                {question.user?.full_name?.charAt(0) || '?'}
              </Avatar>
              
              <Box sx={{ flex: 1, minWidth: 0 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                  <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                    {question.user?.full_name || 'Anonymous'}
                  </Typography>
                  <Chip
                    icon={getStatusIcon(question.status)}
                    label={question.status || 'pending'}
                    size="small"
                    color={getStatusColor(question.status)}
                    variant="outlined"
                  />
                  <Typography variant="caption" color="text.secondary">
                    {formatDistanceToNow(new Date(question.timestamp), { addSuffix: true })}
                  </Typography>
                </Box>
                
                <Typography 
                  variant="body1" 
                  sx={{ 
                    mb: 2,
                    cursor: question.text?.length > 150 ? 'pointer' : 'default'
                  }}
                  onClick={() => question.text?.length > 150 && setExpanded(!expanded)}
                >
                  {expanded || !question.text || question.text.length <= 150
                    ? question.text
                    : `${question.text.substring(0, 150)}...`}
                </Typography>

                {question.answer && (
                  <Paper 
                    elevation={1} 
                    sx={{ 
                      p: 2, 
                      bgcolor: 'primary.light', 
                      color: 'primary.contrastText',
                      borderRadius: 2 
                    }}
                  >
                    <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 1 }}>
                      Host Answer:
                    </Typography>
                    <Typography variant="body2">
                      {question.answer}
                    </Typography>
                    {question.answeredAt && (
                      <Typography variant="caption" sx={{ opacity: 0.8, mt: 1, display: 'block' }}>
                        Answered {formatDistanceToNow(new Date(question.answeredAt), { addSuffix: true })}
                      </Typography>
                    )}
                  </Paper>
                )}

                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 2 }}>
                  <Button
                    size="small"
                    startIcon={<LikeIcon />}
                    onClick={() => onLikeQuestion?.(question.id)}
                    color={hasUserLiked ? 'primary' : 'inherit'}
                    variant={hasUserLiked ? 'contained' : 'outlined'}
                  >
                    {question.likes || 0}
                  </Button>
                  
                  <Button
                    size="small"
                    startIcon={<ReplyIcon />}
                    onClick={() => {/* Handle reply */}}
                    variant="outlined"
                  >
                    Reply
                  </Button>

                  {isHost && question.status !== 'answered' && (
                    <Button
                      size="small"
                      variant="contained"
                      color="primary"
                      onClick={() => setAnswerDialog({ open: true, question })}
                    >
                      Answer
                    </Button>
                  )}
                </Box>
              </Box>

              {canModerate && (
                <Box>
                  <IconButton
                    size="small"
                    onClick={(e) => setMenuAnchor(e.currentTarget)}
                  >
                    <MoreIcon />
                  </IconButton>
                  <Menu
                    anchorEl={menuAnchor}
                    open={Boolean(menuAnchor)}
                    onClose={() => setMenuAnchor(null)}
                  >
                    {isHost && (
                      <MenuItem onClick={() => {
                        // Handle feature/unfeature
                        setMenuAnchor(null);
                      }}>
                        {question.status === 'featured' ? 'Unfeature' : 'Feature'}
                      </MenuItem>
                    )}
                    <MenuItem 
                      onClick={() => {
                        onDeleteQuestion?.(question.id);
                        setMenuAnchor(null);
                      }}
                      sx={{ color: 'error.main' }}
                    >
                      Delete
                    </MenuItem>
                  </Menu>
                </Box>
              )}
            </Box>
          </CardContent>
        </Card>
      </Fade>
    );
  };

  const AllQuestions = () => {
    const allQuestions = filterAndSortQuestions(questions);
    
    return (
      <Box>
        {allQuestions.map((question) => (
          <QuestionCard key={question.id} question={question} />
        ))}
      </Box>
    );
  };

  const PendingQuestions = () => {
    const pendingQuestions = filterAndSortQuestions(questions, 'pending');
    
    return (
      <Box>
        {pendingQuestions.length === 0 ? (
          <Paper sx={{ p: 4, textAlign: 'center', bgcolor: 'grey.50' }}>
            <PendingIcon sx={{ fontSize: 64, color: 'grey.400', mb: 2 }} />
            <Typography variant="h6" color="text.secondary">
              No pending questions
            </Typography>
          </Paper>
        ) : (
          pendingQuestions.map((question) => (
            <QuestionCard key={question.id} question={question} />
          ))
        )}
      </Box>
    );
  };

  const AnsweredQuestions = () => {
    const answeredQuestions = filterAndSortQuestions(questions, 'answered');
    
    return (
      <Box>
        {answeredQuestions.length === 0 ? (
          <Paper sx={{ p: 4, textAlign: 'center', bgcolor: 'grey.50' }}>
            <AnsweredIcon sx={{ fontSize: 64, color: 'grey.400', mb: 2 }} />
            <Typography variant="h6" color="text.secondary">
              No answered questions yet
            </Typography>
          </Paper>
        ) : (
          answeredQuestions.map((question) => (
            <QuestionCard key={question.id} question={question} />
          ))
        )}
      </Box>
    );
  };

  const pendingCount = questions.filter(q => q.status === 'pending').length;
  const answeredCount = questions.filter(q => q.status === 'answered').length;

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <Paper elevation={2} sx={{ 
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        color: 'white',
        p: 2
      }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Typography variant="h6" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <QnaIcon />
            Q&A Session
          </Typography>
          
          <Box sx={{ display: 'flex', gap: 1 }}>
            <IconButton
              onClick={(e) => setSortMenuAnchor(e.currentTarget)}
              sx={{ color: 'white' }}
            >
              <SortIcon />
            </IconButton>
            <Menu
              anchorEl={sortMenuAnchor}
              open={Boolean(sortMenuAnchor)}
              onClose={() => setSortMenuAnchor(null)}
            >
              <MenuItem onClick={() => { setSortBy('recent'); setSortMenuAnchor(null); }}>
                Most Recent
              </MenuItem>
              <MenuItem onClick={() => { setSortBy('popular'); setSortMenuAnchor(null); }}>
                Most Popular
              </MenuItem>
              <MenuItem onClick={() => { setSortBy('answered'); setSortMenuAnchor(null); }}>
                Answered First
              </MenuItem>
            </Menu>
          </Box>
        </Box>

        {/* Search */}
        <TextField
          size="small"
          placeholder="Search questions..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          InputProps={{
            startAdornment: <SearchIcon sx={{ color: 'grey.400', mr: 1 }} />,
            sx: { bgcolor: 'rgba(255,255,255,0.1)', color: 'white' }
          }}
          sx={{ mb: 2, width: '100%' }}
        />

        {/* Question Input */}
        <Box sx={{ display: 'flex', gap: 1 }}>
          <TextField
            size="small"
            placeholder="Ask a question..."
            value={newQuestion}
            onChange={(e) => setNewQuestion(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && handleSubmitQuestion()}
            sx={{ 
              flex: 1,
              '& .MuiOutlinedInput-root': {
                bgcolor: 'rgba(255,255,255,0.1)',
                color: 'white'
              }
            }}
          />
          <Button
            variant="contained"
            onClick={handleSubmitQuestion}
            disabled={!newQuestion.trim()}
            sx={{
              bgcolor: 'rgba(255,255,255,0.2)',
              '&:hover': { bgcolor: 'rgba(255,255,255,0.3)' }
            }}
          >
            <SendIcon />
          </Button>
        </Box>
      </Paper>

      {/* Tabs */}
      <Tabs
        value={activeTab}
        onChange={(e, newValue) => setActiveTab(newValue)}
        sx={{ borderBottom: 1, borderColor: 'divider' }}
      >
        <Tab 
          label="All Questions" 
          icon={<Badge badgeContent={questions.length} color="primary"><QnaIcon /></Badge>} 
          iconPosition="start"
        />
        <Tab 
          label="Pending" 
          icon={<Badge badgeContent={pendingCount} color="warning"><PendingIcon /></Badge>} 
          iconPosition="start"
        />
        <Tab 
          label="Answered" 
          icon={<Badge badgeContent={answeredCount} color="success"><AnsweredIcon /></Badge>} 
          iconPosition="start"
        />
      </Tabs>

      {/* Content */}
      <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
        {activeTab === 0 && <AllQuestions />}
        {activeTab === 1 && <PendingQuestions />}
        {activeTab === 2 && <AnsweredQuestions />}
      </Box>

      {/* Answer Dialog */}
      <Dialog
        open={answerDialog.open}
        onClose={() => setAnswerDialog({ open: false, question: null })}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>
          <Typography variant="h6" component="div">
            Answer Question
          </Typography>
        </DialogTitle>
        <DialogContent>
          {answerDialog.question && (
            <Box sx={{ mb: 2, p: 2, bgcolor: 'grey.50', borderRadius: 1 }}>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                Question from {answerDialog.question.user?.full_name}:
              </Typography>
              <Typography variant="body1">
                {answerDialog.question.text}
              </Typography>
            </Box>
          )}
          <TextField
            autoFocus
            margin="dense"
            label="Your Answer"
            fullWidth
            multiline
            rows={4}
            variant="outlined"
            value={answerText}
            onChange={(e) => setAnswerText(e.target.value)}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setAnswerDialog({ open: false, question: null })}>
            Cancel
          </Button>
          <Button 
            onClick={handleAnswerQuestion}
            variant="contained"
            disabled={!answerText.trim()}
          >
            Submit Answer
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default QnASection;