// utils/reactionConstants.js
export const REACTION_TYPES = {
  THUMBS_UP: 'thumbs_up',
  THUMBS_DOWN: 'thumbs_down',
  HAPPY: 'happy',
  SURPRISED: 'surprised',
  LOVE: 'love',
  CLAP: 'clap',
  RAISE_HAND: 'raise_hand',
  CELEBRATE: 'celebrate',
};

export const REACTION_EMOJIS = {
  [REACTION_TYPES.THUMBS_UP]: '👍',
  [REACTION_TYPES.THUMBS_DOWN]: '👎',
  [REACTION_TYPES.HAPPY]: '😀',
  [REACTION_TYPES.SURPRISED]: '😮',
  [REACTION_TYPES.LOVE]: '❤️',
  [REACTION_TYPES.CLAP]: '👏',
  [REACTION_TYPES.RAISE_HAND]: '✋',
  [REACTION_TYPES.CELEBRATE]: '🎉',
};

export const REACTION_SOUNDS = {
  [REACTION_TYPES.THUMBS_UP]: '/sounds/reaction.mp3',
  [REACTION_TYPES.THUMBS_DOWN]: '/sounds/reaction.mp3',
  [REACTION_TYPES.HAPPY]: '/sounds/reaction.mp3',
  [REACTION_TYPES.SURPRISED]: '/sounds/reaction.mp3',
  [REACTION_TYPES.LOVE]: '/sounds/reaction.mp3',
  [REACTION_TYPES.CLAP]: '/sounds/reaction.mp3',
  [REACTION_TYPES.RAISE_HAND]: '/sounds/hand-raise.mp3',
  [REACTION_TYPES.CELEBRATE]: '/sounds/reaction.mp3',
};

export const REACTION_COLORS = {
  [REACTION_TYPES.THUMBS_UP]: '#4caf50',
  [REACTION_TYPES.THUMBS_DOWN]: '#f44336',
  [REACTION_TYPES.HAPPY]: '#ffeb3b',
  [REACTION_TYPES.SURPRISED]: '#ff9800',
  [REACTION_TYPES.LOVE]: '#e91e63',
  [REACTION_TYPES.CLAP]: '#2196f3',
  [REACTION_TYPES.RAISE_HAND]: '#9c27b0',
  [REACTION_TYPES.CELEBRATE]: '#ff5722',
};