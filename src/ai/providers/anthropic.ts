import { AIProvider } from './provider';

export class AnthropicProvider implements AIProvider {
    async complete(_prompt: string): Promise<string> { return ''; }
}
