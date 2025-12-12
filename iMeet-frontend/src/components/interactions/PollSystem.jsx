import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  TextField,
  Radio,
  RadioGroup,
  FormControlLabel,
  FormControl,
  FormLabel,
  Checkbox,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  LinearProgress,
  Chip,
  Avatar,
  List,
  ListItem,
  ListItemAvatar,
  ListItemText,
  Divider,
  Tabs,
  Tab,
  Paper,
  Fade,
  Zoom
} from '@mui/material';
import {
  Poll as PollIcon,
  Add as AddIcon,
  Delete as DeleteIcon,
  BarChart as ResultsIcon,
  AccessTime as TimerIcon,
  People as ParticipantsIcon,
  CheckCircle as VotedIcon,
  RadioButtonUnchecked as UnvotedIcon,
  Close as CloseIcon
} from '@mui/icons-material';

const PollSystem = ({ 
  polls = [], 
  onCreatePoll, 
  onVote, 
  onClosePoll,
  isHost = false,
  currentUserId 
}) => {
  const [activeTab, setActiveTab] = useState(0);
  const [createDialogOpen, setCreateDialogOpen] = useState(false);
  const [newPoll, setNewPoll] = useState({
    question: '',
    options: ['', ''],
    allowMultiple: false,
    duration: 60, // seconds
    anonymous: false
  });
  const [selectedPoll, setSelectedPoll] = useState(null);
  const [userVotes, setUserVotes] = useState({});

  useEffect(() => {
    // Initialize user votes from polls data
    const votes = {};
    polls.forEach(poll => {
      if (poll.userVote) {
        votes[poll.id] = poll.userVote;
      }
    });
    setUserVotes(votes);
  }, [polls]);

  const handleCreatePoll = () => {
    if (newPoll.question.trim() && newPoll.options.filter(opt => opt.trim()).length >= 2) {
      const pollData = {
        ...newPoll,
        options: newPoll.options.filter(opt => opt.trim()),
        createdBy: currentUserId,
        timestamp: new Date().toISOString()
      };
      onCreatePoll?.(pollData);
      setNewPoll({
        question: '',
        options: ['', ''],
        allowMultiple: false,
        duration: 60,
        anonymous: false
      });
      setCreateDialogOpen(false);
    }
  };

  const handleVote = (pollId, optionIndex, checked = true) => {
    const poll = polls.find(p => p.id === pollId);
    if (!poll) return;

    let newVote;
    if (poll.allowMultiple) {
      const currentVote = userVotes[pollId] || [];
      if (checked) {
        newVote = [...currentVote, optionIndex];
      } else {
        newVote = currentVote.filter(idx => idx !== optionIndex);
      }
    } else {
      newVote = [optionIndex];
    }

    setUserVotes(prev => ({ ...prev, [pollId]: newVote }));
    onVote?.(pollId, newVote);
  };

  const addOption = () => {
    setNewPoll(prev => ({
      ...prev,
      options: [...prev.options, '']
    }));
  };

  const removeOption = (index) => {
    setNewPoll(prev => ({
      ...prev,
      options: prev.options.filter((_, i) => i !== index)
    }));
  };

  const updateOption = (index, value) => {
    setNewPoll(prev => ({
      ...prev,
      options: prev.options.map((opt, i) => i === index ? value : opt)
    }));
  };

  const getVotePercentage = (poll, optionIndex) => {
    const totalVotes = poll.votes?.reduce((sum, count) => sum + count, 0) || 0;
    const optionVotes = poll.votes?.[optionIndex] || 0;
    return totalVotes > 0 ? Math.round((optionVotes / totalVotes) * 100) : 0;
  };

  const hasUserVoted = (pollId) => {
    return userVotes[pollId] && userVotes[pollId].length > 0;
  };

  const getTimeRemaining = (poll) => {
    if (!poll.expiresAt) return null;
    const remaining = new Date(poll.expiresAt) - new Date();
    return Math.max(0, Math.floor(remaining / 1000));
  };

  const ActivePolls = () => {
    const activePolls = polls.filter(poll => poll.status === 'active');
    
    return (
      <Box sx={{ p: 2 }}>
        {activePolls.length === 0 ? (
          <Paper sx={{ p: 4, textAlign: 'center', bgcolor: 'grey.50' }}>
            <PollIcon sx={{ fontSize: 64, color: 'grey.400', mb: 2 }} />
            <Typography variant="h6" color="text.secondary">
              No active polls
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              {isHost ? 'Create a poll to engage participants' : 'Wait for the host to start a poll'}
            </Typography>
          </Paper>
        ) : (
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            {activePolls.map((poll) => (
              <Fade in={true} key={poll.id}>
                <Card elevation={3} sx={{ 
                  background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                  color: 'white'
                }}>
                  <CardContent>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 2 }}>
                      <Typography variant="h6" sx={{ fontWeight: 600, flex: 1 }}>
                        {poll.question}
                      </Typography>
                      {isHost && (
                        <IconButton
                          size="small"
                          onClick={() => onClosePoll?.(poll.id)}
                          sx={{ color: 'rgba(255,255,255,0.8)' }}
                        >
                          <CloseIcon />
                        </IconButton>
                      )}
                    </Box>

                    <Box sx={{ mb: 2, display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                      <Chip
                        icon={<TimerIcon />}
                        label={`${getTimeRemaining(poll) || 0}s left`}
                        size="small"
                        variant="outlined"
                        sx={{ color: 'white', borderColor: 'rgba(255,255,255,0.5)' }}
                      />
                      <Chip
                        icon={<ParticipantsIcon />}
                        label={`${poll.totalVotes || 0} votes`}
                        size="small"
                        variant="outlined"
                        sx={{ color: 'white', borderColor: 'rgba(255,255,255,0.5)' }}
                      />
                      {hasUserVoted(poll.id) && (
                        <Chip
                          icon={<VotedIcon />}
                          label="Voted"
                          size="small"
                          color="success"
                        />
                      )}
                    </Box>

                    <FormControl component="fieldset" sx={{ width: '100%' }}>
                      {poll.allowMultiple ? (
                        <Box>
                          {poll.options.map((option, index) => (
                            <FormControlLabel
                              key={index}
                              control={
                                <Checkbox
                                  checked={userVotes[poll.id]?.includes(index) || false}
                                  onChange={(e) => handleVote(poll.id, index, e.target.checked)}
                                  sx={{ color: 'white', '&.Mui-checked': { color: 'white' } }}
                                />
                              }
                              label={option}
                              sx={{ 
                                width: '100%', 
                                color: 'white',
                                bgcolor: 'rgba(255,255,255,0.1)',
                                borderRadius: 1,
                                mb: 1,
                                mx: 0,
                                pr: 2
                              }}
                            />
                          ))}
                        </Box>
                      ) : (
                        <RadioGroup
                          value={userVotes[poll.id]?.[0]?.toString() || ''}
                          onChange={(e) => handleVote(poll.id, parseInt(e.target.value))}
                        >
                          {poll.options.map((option, index) => (
                            <FormControlLabel
                              key={index}
                              value={index.toString()}
                              control={<Radio sx={{ color: 'white', '&.Mui-checked': { color: 'white' } }} />}
                              label={option}
                              sx={{ 
                                width: '100%', 
                                color: 'white',
                                bgcolor: 'rgba(255,255,255,0.1)',
                                borderRadius: 1,
                                mb: 1,
                                mx: 0,
                                pr: 2
                              }}
                            />
                          ))}
                        </RadioGroup>
                      )}
                    </FormControl>
                  </CardContent>
                </Card>
              </Fade>
            ))}
          </Box>
        )}
      </Box>
    );
  };

  const PollResults = () => {
    const completedPolls = polls.filter(poll => poll.status === 'completed');
    
    return (
      <Box sx={{ p: 2 }}>
        {completedPolls.length === 0 ? (
          <Paper sx={{ p: 4, textAlign: 'center', bgcolor: 'grey.50' }}>
            <ResultsIcon sx={{ fontSize: 64, color: 'grey.400', mb: 2 }} />
            <Typography variant="h6" color="text.secondary">
              No completed polls
            </Typography>
          </Paper>
        ) : (
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            {completedPolls.map((poll) => (
              <Card key={poll.id} elevation={2}>
                <CardContent>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    {poll.question}
                  </Typography>
                  
                  <Box sx={{ mb: 2 }}>
                    <Typography variant="body2" color="text.secondary">
                      Total votes: {poll.totalVotes || 0} • 
                      Completed: {new Date(poll.completedAt).toLocaleString()}
                    </Typography>
                  </Box>

                  {poll.options.map((option, index) => {
                    const percentage = getVotePercentage(poll, index);
                    const votes = poll.votes?.[index] || 0;
                    
                    return (
                      <Box key={index} sx={{ mb: 2 }}>
                        <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                          <Typography variant="body2">{option}</Typography>
                          <Typography variant="body2" color="text.secondary">
                            {votes} votes ({percentage}%)
                          </Typography>
                        </Box>
                        <LinearProgress
                          variant="determinate"
                          value={percentage}
                          sx={{
                            height: 8,
                            borderRadius: 4,
                            bgcolor: 'grey.200',
                            '& .MuiLinearProgress-bar': {
                              borderRadius: 4,
                              background: `linear-gradient(90deg, 
                                ${index % 4 === 0 ? '#4caf50' : 
                                  index % 4 === 1 ? '#2196f3' : 
                                  index % 4 === 2 ? '#ff9800' : '#9c27b0'} 0%, 
                                ${index % 4 === 0 ? '#66bb6a' : 
                                  index % 4 === 1 ? '#42a5f5' : 
                                  index % 4 === 2 ? '#ffb74d' : '#ba68c8'} 100%)`
                            }
                          }}
                        />
                      </Box>
                    );
                  })}

                  {!poll.anonymous && poll.voters && (
                    <Box sx={{ mt: 2 }}>
                      <Typography variant="subtitle2" sx={{ mb: 1 }}>
                        Participants:
                      </Typography>
                      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                        {poll.voters.map((voter, index) => (
                          <Chip
                            key={index}
                            avatar={<Avatar src={voter.profile_picture}>{voter.full_name?.charAt(0)}</Avatar>}
                            label={voter.full_name}
                            size="small"
                            variant="outlined"
                          />
                        ))}
                      </Box>
                    </Box>
                  )}
                </CardContent>
              </Card>
            ))}
          </Box>
        )}
      </Box>
    );
  };

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Paper elevation={2} sx={{ 
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        color: 'white',
        p: 2
      }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h6" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <PollIcon />
            Meeting Polls
          </Typography>
          {isHost && (
            <Button
              variant="contained"
              startIcon={<AddIcon />}
              onClick={() => setCreateDialogOpen(true)}
              sx={{
                bgcolor: 'rgba(255,255,255,0.2)',
                '&:hover': { bgcolor: 'rgba(255,255,255,0.3)' }
              }}
            >
              Create Poll
            </Button>
          )}
        </Box>
      </Paper>

      <Tabs
        value={activeTab}
        onChange={(e, newValue) => setActiveTab(newValue)}
        sx={{ borderBottom: 1, borderColor: 'divider' }}
      >
        <Tab 
          label="Active Polls" 
          icon={<PollIcon />} 
          iconPosition="start"
        />
        <Tab 
          label="Results" 
          icon={<ResultsIcon />} 
          iconPosition="start"
        />
      </Tabs>

      <Box sx={{ flex: 1, overflow: 'auto' }}>
        {activeTab === 0 && <ActivePolls />}
        {activeTab === 1 && <PollResults />}
      </Box>

      {/* Create Poll Dialog */}
      <Dialog
        open={createDialogOpen}
        onClose={() => setCreateDialogOpen(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>
          <Typography variant="h6" component="div" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <PollIcon color="primary" />
            Create New Poll
          </Typography>
        </DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            margin="dense"
            label="Poll Question"
            fullWidth
            variant="outlined"
            value={newPoll.question}
            onChange={(e) => setNewPoll(prev => ({ ...prev, question: e.target.value }))}
            sx={{ mb: 2 }}
          />

          <Typography variant="subtitle2" sx={{ mb: 1 }}>
            Options:
          </Typography>
          {newPoll.options.map((option, index) => (
            <Box key={index} sx={{ display: 'flex', gap: 1, mb: 1 }}>
              <TextField
                size="small"
                label={`Option ${index + 1}`}
                fullWidth
                value={option}
                onChange={(e) => updateOption(index, e.target.value)}
              />
              {newPoll.options.length > 2 && (
                <IconButton
                  onClick={() => removeOption(index)}
                  color="error"
                  size="small"
                >
                  <DeleteIcon />
                </IconButton>
              )}
            </Box>
          ))}

          <Button
            onClick={addOption}
            startIcon={<AddIcon />}
            variant="outlined"
            size="small"
            sx={{ mb: 2 }}
          >
            Add Option
          </Button>

          <Box sx={{ display: 'flex', gap: 2, mb: 2 }}>
            <FormControlLabel
              control={
                <Checkbox
                  checked={newPoll.allowMultiple}
                  onChange={(e) => setNewPoll(prev => ({ ...prev, allowMultiple: e.target.checked }))}
                />
              }
              label="Allow multiple selections"
            />
            <FormControlLabel
              control={
                <Checkbox
                  checked={newPoll.anonymous}
                  onChange={(e) => setNewPoll(prev => ({ ...prev, anonymous: e.target.checked }))}
                />
              }
              label="Anonymous voting"
            />
          </Box>

          <TextField
            type="number"
            label="Duration (seconds)"
            value={newPoll.duration}
            onChange={(e) => setNewPoll(prev => ({ ...prev, duration: parseInt(e.target.value) }))}
            inputProps={{ min: 10, max: 600 }}
            sx={{ width: 200 }}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCreateDialogOpen(false)}>
            Cancel
          </Button>
          <Button 
            onClick={handleCreatePoll}
            variant="contained"
            disabled={!newPoll.question.trim() || newPoll.options.filter(opt => opt.trim()).length < 2}
          >
            Create Poll
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default PollSystem;