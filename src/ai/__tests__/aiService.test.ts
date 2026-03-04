import { MockAIProvider, openAiProviderSettings } from '../aiService';
import * as vscode from 'vscode';

jest.mock('vscode');

describe('MockAIProvider', () => {
  let provider: MockAIProvider;

  beforeEach(() => {
    provider = new MockAIProvider();
  });

  describe('generateCompletion', () => {
    it('should return mock response', async () => {
      const response = await provider.generateCompletion('test prompt');

      expect(response).toBeTruthy();
      expect(response).toContain('mock response');
      expect(response).toContain('Configure an AI provider');
    });

    it('should include standard sections', async () => {
      const response = await provider.generateCompletion('test prompt');

      expect(response).toContain('# What this query answers');
      expect(response).toContain('# Inputs');
      expect(response).toContain('# Business logic');
      expect(response).toContain('# Output');
      expect(response).toContain('# Caveats');
      expect(response).toContain('# Performance notes');
    });

    it('should simulate async operation', async () => {
      const start = Date.now();
      await provider.generateCompletion('test prompt');
      const elapsed = Date.now() - start;

      // Should take at least 500ms (simulated delay)
      expect(elapsed).toBeGreaterThanOrEqual(400); // Allow some variance
    });

    it('should work with any prompt', async () => {
      const prompts = [
        'Explain this SQL',
        'Generate documentation',
        'Add inline comments',
        ''
      ];

      for (const prompt of prompts) {
        const response = await provider.generateCompletion(prompt);
        expect(response).toBeTruthy();
      }
    });
  });
});

describe('openAiProviderSettings', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should execute agent open command if available', async () => {
    (vscode.commands.getCommands as jest.Mock).mockResolvedValue([
      'dp.agent.open',
      'other.command'
    ]);
    (vscode.commands.executeCommand as jest.Mock).mockResolvedValue(undefined);

    await openAiProviderSettings();

    expect(vscode.commands.executeCommand).toHaveBeenCalledWith('dp.agent.open');
  });

  it('should try agentPanel.open if agent.open not found', async () => {
    (vscode.commands.getCommands as jest.Mock).mockResolvedValue([
      'dp.agentPanel.open',
      'other.command'
    ]);
    (vscode.commands.executeCommand as jest.Mock).mockResolvedValue(undefined);

    await openAiProviderSettings();

    expect(vscode.commands.executeCommand).toHaveBeenCalledWith('dp.agentPanel.open');
  });

  it('should try agent.show if other commands not found', async () => {
    (vscode.commands.getCommands as jest.Mock).mockResolvedValue([
      'dp.agent.show',
      'other.command'
    ]);
    (vscode.commands.executeCommand as jest.Mock).mockResolvedValue(undefined);

    await openAiProviderSettings();

    expect(vscode.commands.executeCommand).toHaveBeenCalledWith('dp.agent.show');
  });

  it('should fallback to settings if no agent commands found', async () => {
    (vscode.commands.getCommands as jest.Mock).mockResolvedValue([
      'other.command'
    ]);
    (vscode.commands.executeCommand as jest.Mock).mockResolvedValue(undefined);

    await openAiProviderSettings();

    expect(vscode.commands.executeCommand).toHaveBeenCalledWith(
      'workbench.action.openSettings',
      'dp.ai'
    );
  });

  it('should check commands in priority order', async () => {
    (vscode.commands.getCommands as jest.Mock).mockResolvedValue([
      'dp.agent.open',
      'dp.agentPanel.open',
      'dp.agent.show'
    ]);
    (vscode.commands.executeCommand as jest.Mock).mockResolvedValue(undefined);

    await openAiProviderSettings();

    // Should use the first one found (dp.agent.open)
    expect(vscode.commands.executeCommand).toHaveBeenCalledWith('dp.agent.open');
    expect(vscode.commands.executeCommand).not.toHaveBeenCalledWith('dp.agentPanel.open');
    expect(vscode.commands.executeCommand).not.toHaveBeenCalledWith('dp.agent.show');
  });

  it('should handle getCommands errors', async () => {
    (vscode.commands.getCommands as jest.Mock).mockRejectedValue(new Error('Failed to get commands'));
    (vscode.commands.executeCommand as jest.Mock).mockResolvedValue(undefined);

    await expect(openAiProviderSettings()).rejects.toThrow('Failed to get commands');
  });

  it('should handle executeCommand errors', async () => {
    (vscode.commands.getCommands as jest.Mock).mockResolvedValue(['dp.agent.open']);
    (vscode.commands.executeCommand as jest.Mock).mockRejectedValue(new Error('Failed to execute'));

    await expect(openAiProviderSettings()).rejects.toThrow('Failed to execute');
  });
});
